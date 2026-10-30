[CmdletBinding()]
param(
    [string]$Profile = "AWS-tech-challenge-terraform",
    [string]$Region = "us-east-2",
    [string]$Bucket = "tc-alfabetizacao-a9455abd-lake",
    [string]$JobName = "tc-alfabetizacao-a9455abd-medallion-pipeline",
    [string]$ExpectedAccountId = $env:AWS_EXPECTED_ACCOUNT_ID
)

$ErrorActionPreference = "Stop"

function Invoke-AwsJson {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $output = & aws @Arguments
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        throw "AWS CLI falhou (código $exitCode): aws $($Arguments -join ' ')"
    }

    return ($output | Out-String).Trim()
}

function Get-S3Keys {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prefix
    )

    $raw = Invoke-AwsJson -Arguments @(
        "s3api", "list-objects-v2",
        "--bucket", $Bucket,
        "--prefix", $Prefix,
        "--query", "Contents[].Key",
        "--output", "text",
        "--profile", $Profile,
        "--region", $Region
    )

    if ([string]::IsNullOrWhiteSpace($raw) -or $raw -eq "None") {
        return @()
    }

    # Os nomes de objetos deste projeto não contêm espaços.
    return @($raw -split "\s+" | Where-Object { $_ -and $_ -ne "None" })
}

# -----------------------------------------------------------------------------
# 1. Pré-validação local e da conta AWS
# -----------------------------------------------------------------------------

if ([string]::IsNullOrWhiteSpace($ExpectedAccountId)) {
    throw "Defina AWS_EXPECTED_ACCOUNT_ID antes de executar o teste. Exemplo: `$env:AWS_EXPECTED_ACCOUNT_ID = Read-Host 'ID esperado da conta AWS'"
}

if ($ExpectedAccountId -notmatch '^\d{12}$') {
    throw "AWS_EXPECTED_ACCOUNT_ID deve conter exatamente 12 dígitos."
}

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    throw "AWS CLI não encontrado no PATH."
}

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python não encontrado no PATH. Ative o ambiente virtual do projeto."
}

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$SourceRoot = Join-Path $ProjectRoot "data\source"

$RequiredFiles = @(
    "uf.csv",
    "municipio.csv",
    "meta_alfabetizacao_brasil.csv",
    "meta_alfabetizacao_uf.csv",
    "meta_alfabetizacao_municipio.csv",
    "alunos.csv",
    "dicionario.csv"
)

foreach ($file in $RequiredFiles) {
    $path = Join-Path $SourceRoot $file
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Fonte canônica ausente: $path"
    }
}

$identity = Invoke-AwsJson -Arguments @(
    "sts", "get-caller-identity",
    "--profile", $Profile,
    "--region", $Region,
    "--output", "json"
) | ConvertFrom-Json

if ($identity.Account -ne $ExpectedAccountId) {
    throw "A conta AWS autenticada não corresponde à conta esperada. Nenhum arquivo foi enviado."
}

Write-Host "Identidade AWS validada." -ForegroundColor Green
Write-Host "Perfil: $Profile | Região: $Region | Bucket: $Bucket"

# -----------------------------------------------------------------------------
# 2. Criar cópia de teste e inserir exatamente uma duplicidade
# -----------------------------------------------------------------------------

$testId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ") + "-" + ([guid]::NewGuid().ToString("N").Substring(0, 8))
$tempRoot = Join-Path $env:TEMP "tc-glue-quality-gate-$testId"
$testSource = Join-Path $tempRoot "source"

New-Item -ItemType Directory -Path $testSource -Force | Out-Null

try {
    foreach ($file in $RequiredFiles) {
        Copy-Item -LiteralPath (Join-Path $SourceRoot $file) -Destination $testSource
    }

    $copiedFiles = @(Get-ChildItem -LiteralPath $testSource -File)
    if ($copiedFiles.Count -ne $RequiredFiles.Count) {
        throw "A quantidade de arquivos copiados não corresponde às sete fontes esperadas."
    }

    $municipioTestFile = Join-Path $testSource "municipio.csv"
    $env:QUALITY_GATE_TEST_MUNICIPIO = $municipioTestFile

    $pythonCode = @'
import os
import sys
import pandas as pd

path = os.environ["QUALITY_GATE_TEST_MUNICIPIO"]
keys = ["ano", "id_municipio", "serie", "rede"]

try:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
except Exception as exc:
    print(f"Falha ao ler municipio.csv: {exc}", file=sys.stderr)
    raise SystemExit(1)

missing = [column for column in keys if column not in df.columns]
if missing:
    print(f"Colunas da chave natural ausentes: {missing}", file=sys.stderr)
    raise SystemExit(1)

if df.empty:
    print("municipio.csv está vazio", file=sys.stderr)
    raise SystemExit(1)

eligible = df.copy()
for column in keys:
    eligible = eligible[eligible[column].astype(str).str.strip().ne("")]

if eligible.empty:
    print("Não existe linha com chave natural completa para duplicar", file=sys.stderr)
    raise SystemExit(1)

# Verifica duplicidades somente entre chaves naturais completas.
baseline_duplicates = int(eligible.duplicated(subset=keys).sum())
if baseline_duplicates != 0:
    print(
        f"A fonte canônica já contém {baseline_duplicates} duplicidade(s) "
        "com chave natural completa. O teste foi interrompido para não confundir resultados.",
        file=sys.stderr,
    )
    raise SystemExit(1)

sample = eligible.iloc[[0]].copy()
df = pd.concat([df, sample], ignore_index=True)
df.to_csv(path, index=False, encoding="utf-8")

eligible_after = df.copy()
for column in keys:
    eligible_after = eligible_after[eligible_after[column].astype(str).str.strip().ne("")]

duplicates = int(eligible_after.duplicated(subset=keys).sum())
if duplicates != 1:
    print(
        f"Esperava-se exatamente uma duplicidade, mas foram encontradas {duplicates}.",
        file=sys.stderr,
    )
    raise SystemExit(1)

print(f"Teste preparado: {len(df)} linhas; exatamente uma duplicidade na chave {keys}.")
'@

    $pythonExitCode = 0
    try {
        $pythonCode | python -
        $pythonExitCode = $LASTEXITCODE
    }
    finally {
        Remove-Item Env:\QUALITY_GATE_TEST_MUNICIPIO -ErrorAction SilentlyContinue
    }

    if ($pythonExitCode -ne 0) {
        throw "Falha ao preparar municipio.csv de teste. Nenhum arquivo de produção foi alterado."
    }

    # -------------------------------------------------------------------------
    # 3. Upload isolado para Bronze e remoção da cópia local de entrada
    # -------------------------------------------------------------------------

    $sourcePrefix = "bronze/quality-gate-tests/$testId/source"
    $streamPrefix = "bronze/quality-gate-tests/$testId/streaming"
    $silverPrefix = "silver/quality-gate-tests/$testId"
    $goldPrefix = "gold/quality-gate-tests/$testId"

    # Captura o estado anterior para identificar o relatório criado por esta execução.
    $qualityBefore = @(Get-S3Keys -Prefix "observability/quality/")

    Write-Host "Enviando as sete fontes para o prefixo isolado de teste: s3://$Bucket/$sourcePrefix/"
    Invoke-AwsJson -Arguments @(
        "s3", "sync",
        $testSource,
        "s3://$Bucket/$sourcePrefix/",
        "--profile", $Profile,
        "--region", $Region
    ) | Out-Null

    # Remove somente a cópia local de teste; data/source permanece intacto.
    Remove-Item -LiteralPath $testSource -Recurse -Force

    # -------------------------------------------------------------------------
    # 4. Iniciar o mesmo Glue Job com prefixos de saída exclusivos
    # -------------------------------------------------------------------------

    $arguments = [ordered]@{
        "--SOURCE_PREFIX" = "s3://$Bucket/$sourcePrefix"
        "--STREAM_PREFIX" = "s3://$Bucket/$streamPrefix"
        "--OUTPUT_PREFIX" = "s3://$Bucket/$silverPrefix"
        "--GOLD_PREFIX" = "s3://$Bucket/$goldPrefix"
        "--MIN_JOIN_MATCH_RATE" = "0.95"
        "--MAX_INVALID_WEIGHT_RATE" = "0.001"
    }

    $startRequest = [ordered]@{
        JobName   = $JobName
        Arguments = $arguments
    }

    $startRequestPath = Join-Path $tempRoot "start_job_request.json"
    $startRequestJson = $startRequest | ConvertTo-Json -Depth 10
    $jsonBytes = [System.Text.Encoding]::UTF8.GetBytes($startRequestJson)
    [System.IO.File]::WriteAllBytes($startRequestPath, $jsonBytes)

    $fileUri = "file://" + ($startRequestPath -replace '\\', '/')

    Write-Host "Iniciando execução de teste do Glue. Silver e Gold usam prefixos exclusivos."

    $startOutput = Invoke-AwsJson -Arguments @(
        "glue", "start-job-run",
        "--cli-input-json", $fileUri,
        "--profile", $Profile,
        "--region", $Region,
        "--output", "json"
    )

    $startResponse = $startOutput | ConvertFrom-Json
    $glueRunId = $startResponse.JobRunId

    if ([string]::IsNullOrWhiteSpace($glueRunId)) {
        throw "AWS Glue não retornou JobRunId."
    }

    Write-Host "Glue JobRunId: $glueRunId"

    # -------------------------------------------------------------------------
    # 5. Aguardar o término da execução
    # -------------------------------------------------------------------------

    $terminalStates = @("SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR", "EXPIRED")
    $state = "STARTING"
    $jobRun = $null

    while ($state -notin $terminalStates) {
        Start-Sleep -Seconds 15

        $runOutput = Invoke-AwsJson -Arguments @(
            "glue", "get-job-run",
            "--job-name", $JobName,
            "--run-id", $glueRunId,
            "--query", "JobRun.{State:JobRunState,Error:ErrorMessage,Started:StartedOn,Completed:CompletedOn}",
            "--profile", $Profile,
            "--region", $Region,
            "--output", "json"
        )

        $jobRun = $runOutput | ConvertFrom-Json
        $state = $jobRun.State

        if ([string]::IsNullOrWhiteSpace($state)) {
            throw "AWS Glue retornou estado vazio para o JobRunId $glueRunId."
        }

        Write-Host "Estado do Glue: $state"
    }

    if ($state -ne "FAILED") {
        $errorDetail = if ($jobRun.Error) { $jobRun.Error } else { "Sem mensagem de erro retornada." }
        throw "O teste deveria terminar em FAILED, mas terminou em $state. Detalhe: $errorDetail"
    }

    # -------------------------------------------------------------------------
    # 6. Localizar e validar o relatório do Quality Gate
    # -------------------------------------------------------------------------

    $qualityAfter = @(Get-S3Keys -Prefix "observability/quality/")
    $newQualityKeys = @($qualityAfter | Where-Object { $qualityBefore -notcontains $_ })

    if ($newQualityKeys.Count -ne 1) {
        $found = if ($newQualityKeys.Count -eq 0) { "nenhum" } else { $newQualityKeys -join ", " }
        throw "Esperava exatamente um novo relatório em observability/quality/, mas encontrei $($newQualityKeys.Count): $found. Verifique execuções concorrentes e os logs do Glue."
    }

    $qualityKey = $newQualityKeys[0]
    $qualityLocal = Join-Path $tempRoot "quality_report.json"

    Invoke-AwsJson -Arguments @(
        "s3", "cp",
        "s3://$Bucket/$qualityKey",
        $qualityLocal,
        "--profile", $Profile,
        "--region", $Region
    ) | Out-Null

    $qualityReport = Get-Content -LiteralPath $qualityLocal -Raw | ConvertFrom-Json

    if ($qualityReport.status -ne "FAILED") {
        throw "Relatório de qualidade não está FAILED: $($qualityReport.status)"
    }

    $duplicateChecks = @(
        $qualityReport.checks |
            Where-Object { $_.check_name -eq "duplicate_natural_keys:municipio" }
    )

    if ($duplicateChecks.Count -ne 1) {
        throw "Esperava exatamente uma verificação duplicate_natural_keys:municipio no relatório; encontrei $($duplicateChecks.Count)."
    }

    $duplicateCheck = $duplicateChecks[0]

    if ($duplicateCheck.status -ne "FAIL") {
        throw "A regra duplicate_natural_keys:municipio não registrou FAIL. Resultado: $($duplicateCheck.status)"
    }

    if ([int]$duplicateCheck.measured -ne 1) {
        throw "Esperava exatamente uma duplicidade detectada; valor registrado: $($duplicateCheck.measured)"
    }

    # -------------------------------------------------------------------------
    # 7. Baixar e validar métricas da execução
    # -------------------------------------------------------------------------

    $runId = $qualityReport.run_id
    if ([string]::IsNullOrWhiteSpace($runId)) {
        throw "O relatório de qualidade não contém run_id."
    }

    $metricsKey = "observability/metrics/run_id=$runId.json"
    $metricsLocal = Join-Path $tempRoot "run_metrics.json"

    Invoke-AwsJson -Arguments @(
        "s3", "cp",
        "s3://$Bucket/$metricsKey",
        $metricsLocal,
        "--profile", $Profile,
        "--region", $Region
    ) | Out-Null

    $metricsReport = Get-Content -LiteralPath $metricsLocal -Raw | ConvertFrom-Json

    if ($metricsReport.status -ne "FAILED") {
        throw "Métricas da execução não registram FAILED: $($metricsReport.status)"
    }

    # -------------------------------------------------------------------------
    # 8. Confirmar que Silver e Gold não receberam objetos
    # -------------------------------------------------------------------------

    $silverKeys = @(Get-S3Keys -Prefix "$silverPrefix/")
    $goldKeys = @(Get-S3Keys -Prefix "$goldPrefix/")

    if ($silverKeys.Count -ne 0 -or $goldKeys.Count -ne 0) {
        throw "Foram encontrados objetos nos prefixos isolados. Silver=$($silverKeys.Count), Gold=$($goldKeys.Count). Investigue antes de concluir."
    }

    # -------------------------------------------------------------------------
    # 9. Resultado
    # -------------------------------------------------------------------------

    Write-Host ""
    Write-Host "QUALITY GATE AWS COMPROVADO" -ForegroundColor Green
    Write-Host "Glue JobRunId: $glueRunId"
    Write-Host "Run ID interno: $runId"
    Write-Host "Regra: $($duplicateCheck.check_name)"
    Write-Host "Resultado: $($duplicateCheck.status)"
    Write-Host "Duplicidades detectadas: $($duplicateCheck.measured)"
    Write-Host "Relatório: s3://$Bucket/$qualityKey"
    Write-Host "Métricas: s3://$Bucket/$metricsKey"
    Write-Host "Objetos Silver publicados no prefixo de teste: $($silverKeys.Count)"
    Write-Host "Objetos Gold publicados no prefixo de teste: $($goldKeys.Count)"
    Write-Host "Fontes temporárias mantidas em: s3://$Bucket/$sourcePrefix/"
    Write-Host "Relatórios e arquivo de solicitação locais em: $tempRoot"
    Write-Host ""
    Write-Host "Nenhuma limpeza automática foi executada no S3. Preserve as evidências e remova manualmente as versões temporárias quando apropriado."
}
catch {
    Write-Warning "O teste foi interrompido: $($_.Exception.Message)"

    if ($sourcePrefix) {
        Write-Warning "Verifique os objetos temporários em: s3://$Bucket/$sourcePrefix/"
    }

    if ($tempRoot -and (Test-Path -LiteralPath $tempRoot)) {
        Write-Warning "Arquivos locais de diagnóstico/evidência: $tempRoot"
    }

    throw
}
finally {
    Remove-Item Env:\QUALITY_GATE_TEST_MUNICIPIO -ErrorAction SilentlyContinue
}
