param(
    [string]$Profile = "AWS-tech-challenge-terraform",
    [string]$Region = "us-east-2",
    [string]$Bucket = "tc-alfabetizacao-a9455abd-lake",
    [string]$JobName = "tc-alfabetizacao-a9455abd-medallion-pipeline",
    [string]$ExpectedAccountId = $env:AWS_EXPECTED_ACCOUNT_ID
)
$ErrorActionPreference = "Stop"

function Invoke-AwsJson {
    param([string[]]$Arguments)

    $output = & aws @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI falhou: aws $($Arguments -join ' ')"
    }
    return ($output | Out-String).Trim()
}

function Get-S3Keys {
    param([string]$Prefix)

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

    return @($raw -split "\s+" | Where-Object { $_ -and $_ -ne "None" })
}

# This script must be stored in <project>\scripts\test_glue_quality_gate.ps1.
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
    if (-not (Test-Path $path)) {
        throw "Fonte canônica ausente: $path"
    }
}

# Validate access before preparing or uploading any test input.
$identity = Invoke-AwsJson -Arguments @(
    "sts", "get-caller-identity",
    "--profile", $Profile,
    "--region", $Region
) | ConvertFrom-Json

Write-Host "Conta AWS autenticada: $($identity.Account)"
Write-Host "Perfil: $Profile | Região: $Region"

if ($identity.Account -ne "262325794007") {
    throw "A conta AWS retornada não é a conta esperada para este projeto. Nenhum teste foi enviado."
}

$testId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ") + "-" + ([guid]::NewGuid().ToString("N").Substring(0, 8))
$tempRoot = Join-Path $env:TEMP "tc-glue-quality-gate-$testId"
$testSource = Join-Path $tempRoot "source"
New-Item -ItemType Directory -Path $testSource -Force | Out-Null

# Copy the canonical local inputs to a temporary directory. The production files are not edited.
Copy-Item (Join-Path $SourceRoot "*.csv") -Destination $testSource

$municipioTestFile = Join-Path $testSource "municipio.csv"
$env:QUALITY_GATE_TEST_MUNICIPIO = $municipioTestFile

$pythonCode = @'
import os
import pandas as pd

path = os.environ["QUALITY_GATE_TEST_MUNICIPIO"]
df = pd.read_csv(path, dtype=str, keep_default_na=False)
keys = ["ano", "id_municipio", "serie", "rede"]

missing = [column for column in keys if column not in df.columns]
if missing:
    raise SystemExit(f"Colunas da chave natural ausentes: {missing}")
if df.empty:
    raise SystemExit("municipio.csv está vazio")

eligible = df.dropna(subset=keys).copy()
for column in keys:
    eligible = eligible[eligible[column].astype(str).str.strip().ne("")]

if eligible.empty:
    raise SystemExit("Não existe linha com chave natural completa para duplicar")

sample = eligible.iloc[[0]].copy()
df = pd.concat([df, sample], ignore_index=True)
df.to_csv(path, index=False, encoding="utf-8")

duplicates = int(df.duplicated(keys).sum())
if duplicates < 1:
    raise SystemExit("Não foi possível criar a duplicidade esperada")

print(f"Teste preparado: {len(df)} linhas; duplicidades na chave {keys}: {duplicates}")
'@

$pythonCode | python -
$pythonExitCode = $LASTEXITCODE

Remove-Item Env:\QUALITY_GATE_TEST_MUNICIPIO -ErrorAction SilentlyContinue

if ($pythonExitCode -ne 0) {
    throw "Falha ao preparar municipio.csv de teste. Nenhum arquivo de produção foi alterado."
}

$sourcePrefix = "bronze/quality-gate-tests/$testId/source"
$streamPrefix = "bronze/quality-gate-tests/$testId/streaming"
$silverPrefix = "silver/quality-gate-tests/$testId"
$goldPrefix = "gold/quality-gate-tests/$testId"

# Snapshot existing quality keys so we can identify the report from this run.
$qualityBefore = @(Get-S3Keys -Prefix "observability/quality/")

Write-Host "Enviando fontes para prefixo isolado de teste: s3://$Bucket/$sourcePrefix/"
Invoke-AwsJson -Arguments @(
    "s3", "sync",
    $testSource,
    "s3://$Bucket/$sourcePrefix/",
    "--profile", $Profile,
    "--region", $Region
) | Out-Null

Remove-Item -LiteralPath $testSource -Recurse -Force

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

Write-Host "Iniciando execução de teste do Glue. Os prefixos Silver/Gold são exclusivos."

$startOutput = Invoke-AwsJson -Arguments @(
    "glue",
    "start-job-run",
    "--cli-input-json",
    $fileUri,
    "--profile",
    $Profile,
    "--region",
    $Region,
    "--output",
    "json"
)

$startResponse = $startOutput | ConvertFrom-Json


$glueRunId = $startResponse.JobRunId
if (-not $glueRunId) {
    throw "AWS Glue não retornou JobRunId."
}
Write-Host "Glue JobRunId: $glueRunId"

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
    Write-Host "Estado do Glue: $state"
}

if ($state -ne "FAILED") {
    throw "O teste deveria terminar em FAILED, mas terminou em $state. Verifique a fonte de teste e o log do Glue."
}

# Identify the unique quality report created by this run.
$qualityAfter = @(Get-S3Keys -Prefix "observability/quality/")
$newQualityKeys = @($qualityAfter | Where-Object { $qualityBefore -notcontains $_ })
if ($newQualityKeys.Count -lt 1) {
    throw "A execução falhou, mas não foi encontrado novo relatório em observability/quality/."
}

$qualityKey = $newQualityKeys | Sort-Object -Descending | Select-Object -First 1
$qualityLocal = Join-Path $tempRoot "quality_report.json"
Invoke-AwsJson -Arguments @(
    "s3", "cp",
    "s3://$Bucket/$qualityKey",
    $qualityLocal,
    "--profile", $Profile,
    "--region", $Region
) | Out-Null

$qualityReport = Get-Content $qualityLocal -Raw | ConvertFrom-Json
$duplicateCheck = $qualityReport.checks |
    Where-Object { $_.check_name -eq "duplicate_natural_keys:municipio" } |
    Select-Object -First 1

if ($qualityReport.status -ne "FAILED") {
    throw "Relatório de qualidade não está FAILED: $($qualityReport.status)"
}
if (-not $duplicateCheck -or $duplicateCheck.status -ne "FAIL") {
    throw "O relatório não comprova a falha de duplicate_natural_keys:municipio."
}

$runId = $qualityReport.run_id
$metricsKey = "observability/metrics/run_id=$runId.json"
$metricsLocal = Join-Path $tempRoot "run_metrics.json"
Invoke-AwsJson -Arguments @(
    "s3", "cp",
    "s3://$Bucket/$metricsKey",
    $metricsLocal,
    "--profile", $Profile,
    "--region", $Region
) | Out-Null

$metricsReport = Get-Content $metricsLocal -Raw | ConvertFrom-Json
if ($metricsReport.status -ne "FAILED") {
    throw "Métricas da execução não registram FAILED: $($metricsReport.status)"
}

$silverKeys = @(Get-S3Keys -Prefix "$silverPrefix/")
$goldKeys = @(Get-S3Keys -Prefix "$goldPrefix/")

if ($silverKeys.Count -ne 0 -or $goldKeys.Count -ne 0) {
    throw "Foram encontrados objetos nos prefixos de saída isolados. Silver=$($silverKeys.Count), Gold=$($goldKeys.Count). Investigue antes de concluir."
}

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
Write-Host "Cópias locais e relatórios baixados em: $tempRoot"
Write-Host ""
Write-Host "Não foi executada limpeza automática. Preserve os relatórios como evidência."
