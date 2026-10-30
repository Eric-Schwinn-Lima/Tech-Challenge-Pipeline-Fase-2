# Guia de implantação e operação AWS

> Este guia documenta a implantação realizada e como reproduzi-la. A criação de recursos pode gerar cobrança. Não execute `apply` ou `destroy` sem revisar o plano e conferir a conta/região.

## 1. Ambiente validado

| Item | Valor |
|---|---|
| Região | `us-east-2` |
| Perfil CLI principal | `AWS-tech-challenge` |
| Perfil para Terraform/SDK | `AWS-tech-challenge-terraform` |
| Perfil Terraform | Utiliza `credential_process` para consumir a sessão autenticada |
| Project name nos tfvars locais | `tc-alfabetizacao` |
| Agendamento automático | Desabilitado (`enable_daily_schedule = false`) |

Os perfis precisam autenticar na mesma conta AWS autorizada para este projeto. Não publique IDs de conta, tokens, credenciais, `terraform.tfvars`, planos ou estados Terraform.

## 2. Pré-requisitos

- Windows PowerShell ou shell compatível.
- AWS CLI v2 com os perfis configurados.
- Terraform compatível com `versions.tf` (>= 1.6 no projeto).
- Python 3.11 ou superior e ambiente virtual do projeto.
- Acesso IAM para S3, IAM, SQS, Lambda, Glue, Step Functions, EventBridge, CloudWatch, SNS e Athena.
- Os seis arquivos oficiais de dados e `dicionario.csv`, obtidos fora do repositório quando não estiverem disponíveis localmente.

## 3. Autenticação

Quando a sessão expirar, autentique o perfil principal:

```powershell
aws login --profile AWS-tech-challenge --region us-east-2
```

Confirme a identidade no perfil que o Terraform utilizará:

```powershell
aws sts get-caller-identity --profile AWS-tech-challenge-terraform
```

Na mesma janela PowerShell, configure o ambiente:

```powershell
$env:AWS_PROFILE = "AWS-tech-challenge-terraform"
$env:AWS_REGION = "us-east-2"
```

Confirme que a AWS CLI responde:

```powershell
aws sts get-caller-identity
```

Se o perfil `AWS-tech-challenge-terraform` não existir ou seu `credential_process` falhar, corrija a configuração local do perfil antes de executar o Terraform. Não copie tokens temporários para arquivos do projeto.

## 4. Preparar e validar as fontes

Os CSVs reais completos não são versionados no repositório público. Coloque o ZIP oficial em `data/exports_raw/` (quando aplicável) e o `dicionario.csv` oficial ao lado dele; ou coloque os seis CSVs brutos com os nomes esperados no mesmo diretório.

```powershell
python scripts/prepare_real_data.py
python scripts/validate_sources.py
python -m pytest -q
python scripts/run_local.py
```

A preparação cria as sete fontes canônicas em `data/source/`: seis entidades analíticas e `dicionario.csv`. Metas são transformadas do formato largo para formato longo, com `ano` como ano-alvo e `meta_snapshot_ano` preservando o ano do snapshot. A UF pode ser derivada do prefixo do código IBGE quando ausente. O inventário registra o alerta de 1.048.575 registros no arquivo de alunos.

Não afirme que a reconciliação por microdados é conclusiva antes de verificar a completude do arquivo original.

## 5. Inicializar e revisar Terraform

Na raiz do repositório:

```powershell
terraform -chdir=cloud/terraform init
terraform -chdir=cloud/terraform fmt -recursive
terraform -chdir=cloud/terraform validate
terraform -chdir=cloud/terraform plan
```

Para a implantação original, `terraform apply` só deve ser executado após revisar o plano e a conta/região. Para a infraestrutura já implantada, não reaplique automaticamente: na verificação após retirar o e-mail do default de `alert_email`, o plano mostrou **0 add, 1 change, 0 destroy**, sendo a única mudança prevista a inclusão de tags no objeto S3 do script Glue. Essa alteração de tags não é necessária para a correção de segurança do endereço de e-mail.

### Configuração privada

`cloud/terraform/terraform.tfvars` é local e ignorado pelo Git. Ele contém as configurações específicas da conta, por exemplo:

```hcl
aws_region  = "us-east-2"
project_name = "tc-alfabetizacao"
alert_email = "SEU_EMAIL_DE_ALERTA"
```

O valor padrão público de `alert_email` em `variables.tf` deve permanecer vazio. Não adicione o e-mail real a `terraform.tfvars.example`.

## 6. Outputs e recursos provisionados

Obtenha os nomes atuais por Terraform, não por cópia manual de outro ambiente:

```powershell
$bucket = terraform -chdir=cloud/terraform output -raw s3_bucket_name
$queueUrl = terraform -chdir=cloud/terraform output -raw sqs_queue_url
$stateMachine = terraform -chdir=cloud/terraform output -raw state_machine_arn
$glueJob = terraform -chdir=cloud/terraform output -raw glue_job_name
$silverCrawler = terraform -chdir=cloud/terraform output -raw silver_crawler_name
$goldCrawler = terraform -chdir=cloud/terraform output -raw gold_crawler_name
```

A infraestrutura documentada inclui bucket S3 versionado/criptografado, SQS Standard, Lambda e event source mapping, Glue Job, roles/policies IAM, Glue Database/Crawlers, Athena Workgroup, Step Functions, CloudWatch Alarm e SNS.

## 7. Carga Bronze

O job Glue espera os sete arquivos canônicos no prefixo `bronze/source/`:

```powershell
aws s3 sync data/source/ "s3://$bucket/bronze/source/" --exclude "*" --include "*.csv"
aws s3 ls "s3://$bucket/bronze/source/" --recursive
```

A pasta `bronze/raw_exports/` é preservação dos arquivos brutos; o job usa os CSVs canônicos em `bronze/source/`.

## 8. Testar a entrada SQS → Lambda → S3

```powershell
python scripts/send_sqs_events.py --queue-url $queueUrl --region us-east-2 --profile AWS-tech-challenge-terraform
aws s3 ls "s3://$bucket/bronze/streaming/" --recursive
```

Verifique CloudWatch Logs da função Lambda e o namespace `AlfabetizacaoPipeline`, métricas `EventsIngested`, `InvalidEvents` e `EventLatencySeconds`.

Os eventos são simulados. A validação real mostrou que os eventos de teste não continham todas as colunas exigidas para um registro analítico de alfabetização; por isso, o processamento Glue os encaminha para quarentena. Não consulte `mart_streaming_eventos` como se ela tivesse sido gerada na execução AWS validada.

## 9. Executar a pipeline

A máquina Step Functions chama o Glue Job de forma síncrona e trata falha com Retry/Catch. Ela **não inicia os crawlers**.

```powershell
$executionArn = aws stepfunctions start-execution `
  --state-machine-arn $stateMachine `
  --input '{}' `
  --query executionArn `
  --output text

Write-Host "Execution ARN: $executionArn"
```

Acompanhe o estado:

```powershell
do {
  Start-Sleep -Seconds 10
  $status = aws stepfunctions describe-execution --execution-arn $executionArn --query status --output text
  Write-Host "Status: $status"
} while ($status -eq "RUNNING")

aws stepfunctions describe-execution --execution-arn $executionArn
```

Após sucesso, execute os crawlers separadamente:

```powershell
aws glue start-crawler --name $silverCrawler
aws glue start-crawler --name $goldCrawler
```

Aguarde ambos retornarem ao estado `READY` e verifique `LastCrawl.Status = SUCCEEDED`.

## 10. Consultar Athena

Selecione o Glue Database e o Workgroup criados pelo Terraform. As tabelas Gold são particionadas por `run_id`; filtre a execução:

```sql
SELECT COUNT(*) AS linhas
FROM fato_alfabetizacao_municipio
WHERE run_id = '20261029T231856Z_37704ef5';

SELECT status_meta_municipio, COUNT(*) AS quantidade
FROM fato_alfabetizacao_municipio
WHERE run_id = '20261029T231856Z_37704ef5'
GROUP BY status_meta_municipio
ORDER BY status_meta_municipio;
```

Registre `Data scanned` em cada consulta para FinOps. Não use uma consulta sem `run_id` para somar execuções históricas.

## 11. Evidências

Use `evidencias_execucao.md` como índice. Registre o novo Job Run ID, duração, estado, contagens, taxas de join, versão do script, estado dos crawlers, query Athena e bytes escaneados. Não substitua resultados históricos; adicione um novo `run_id`.

## 12. Limpeza segura

A limpeza remove dados e recursos. Antes, confirme que as evidências foram exportadas e que o bucket é exclusivamente deste projeto.

```powershell
python scripts/purge_versioned_bucket.py --bucket $bucket --region us-east-2 --confirm
terraform -chdir=cloud/terraform destroy
```

O script de purge remove versões do bucket; é irreversível. Revise cuidadosamente o nome do bucket antes de executá-lo. Após o `destroy`, confira no Console AWS se os recursos foram removidos e acompanhe Billing/Cost Explorer, pois a atualização pode ter atraso.
