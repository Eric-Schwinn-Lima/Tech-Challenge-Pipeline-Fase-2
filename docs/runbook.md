# Runbook operacional

Este runbook descreve as ações usuais para validar, executar, monitorar, diagnosticar e desligar o pipeline. Os comandos assumem Windows PowerShell e região `us-east-2`.

## 1. Contexto e nomes de referência

Os nomes atuais incluem o prefixo `tc-alfabetizacao-a9455abd`, por exemplo:

- Bucket S3: `tc-alfabetizacao-a9455abd-lake`.
- Glue Job: `tc-alfabetizacao-a9455abd-medallion-pipeline`.
- Glue Database: `tc_alfabetizacao_a9455abd_catalog`.
- SQS: `tc-alfabetizacao-a9455abd-events`.
- Lambda: `tc-alfabetizacao-a9455abd-stream-consumer`.
- Step Functions: `tc-alfabetizacao-a9455abd-pipeline-orchestrator`.

Os nomes são dependentes do sufixo aleatório do Terraform. Em outro ambiente, obtenha os nomes por `terraform output`.

## 2. Verificação inicial

```powershell
$env:AWS_PROFILE = "AWS-tech-challenge-terraform"
$env:AWS_REGION = "us-east-2"
aws sts get-caller-identity
terraform -chdir=cloud/terraform validate
terraform -chdir=cloud/terraform plan
```

Confirme conta e região. Um plano que propõe destruição inesperada deve ser interrompido e investigado.

## 3. Validar dados localmente

```powershell
python scripts/validate_sources.py
python -m pytest -q
python scripts/run_local.py
```

Se a preparação ainda não tiver sido feita:

```powershell
python scripts/prepare_real_data.py
```

Não avance sem as sete fontes canônicas esperadas em `data/source/`. Os arquivos completos de microdados não são versionados no repositório público.

## 4. Executar o fluxo batch AWS

Obtenha os outputs:

```powershell
$bucket = terraform -chdir=cloud/terraform output -raw s3_bucket_name
$stateMachine = terraform -chdir=cloud/terraform output -raw state_machine_arn
$silverCrawler = terraform -chdir=cloud/terraform output -raw silver_crawler_name
$goldCrawler = terraform -chdir=cloud/terraform output -raw gold_crawler_name
```

Verifique a carga canônica:

```powershell
aws s3 ls "s3://$bucket/bronze/source/" --recursive
```

Inicie a orquestração:

```powershell
$executionArn = aws stepfunctions start-execution --state-machine-arn $stateMachine --input '{}' --query executionArn --output text
aws stepfunctions describe-execution --execution-arn $executionArn
```

Acompanhe até `SUCCEEDED` ou `FAILED`. A Step Functions executa o Glue Job; os crawlers não fazem parte dessa definição e precisam ser iniciados separadamente.

Após sucesso:

```powershell
aws glue start-crawler --name $silverCrawler
aws glue start-crawler --name $goldCrawler
```

Aguarde os dois crawlers finalizarem. `State = READY` informa que não estão executando; também confirme `LastCrawl.Status = SUCCEEDED`.

## 5. Executar/monitorar streaming de demonstração

```powershell
$queueUrl = terraform -chdir=cloud/terraform output -raw sqs_queue_url
python scripts/send_sqs_events.py --queue-url $queueUrl --region us-east-2 --profile AWS-tech-challenge-terraform
aws s3 ls "s3://$bucket/bronze/streaming/" --recursive
```

Verifique a Lambda no CloudWatch Logs e as métricas no namespace `AlfabetizacaoPipeline`:

- `EventsIngested`
- `InvalidEvents`
- `EventLatencySeconds`

Eventos de demonstração não são dados oficiais. Se o evento não tiver o esquema de alfabetização exigido pelo job Glue, deve ser enviado à quarentena. Não espere uma tabela AWS `mart_streaming_eventos` enquanto o produtor continuar enviando o esquema operacional de teste atual.

## 6. Consultas Athena de sanidade

Selecione o database e o workgroup de Analytics. Exemplo para a execução AWS registrada:

```sql
SELECT COUNT(*)
FROM fato_alfabetizacao_municipio
WHERE run_id = '20261029T231856Z_37704ef5';

SELECT status_meta_municipio, COUNT(*) AS quantidade
FROM fato_alfabetizacao_municipio
WHERE run_id = '20261029T231856Z_37704ef5'
GROUP BY status_meta_municipio
ORDER BY status_meta_municipio;
```

Compare os resultados com `evidencias_execucao.md`. Registre bytes escaneados e não misture execuções diferentes.

## 7. Diagnóstico de falhas

### Terraform não encontra credenciais

1. Renove login do perfil principal se necessário: `aws login --profile AWS-tech-challenge --region us-east-2`.
2. Teste `aws sts get-caller-identity --profile AWS-tech-challenge-terraform`.
3. Defina `$env:AWS_PROFILE = "AWS-tech-challenge-terraform"` na mesma janela PowerShell.
4. Confirme que o perfil Terraform usa o `credential_process` esperado. Não tente usar metadados IMDS em uma máquina local.

### Glue falha por coluna ausente

- Verifique o esquema da mensagem no prefixo `bronze/streaming/`.
- Confirme que o job publicado no S3 corresponde à versão local atual.
- Eventos de teste não devem ser promovidos ao indicador oficial; esquema incompleto deve permanecer em quarentena.
- Consulte CloudWatch Logs e o relatório de qualidade do `run_id`.

### Crawler não conclui

- Confira se a execução Glue terminou e publicou arquivos em Silver/Gold.
- Verifique o caminho S3 configurado no crawler e permissões de leitura/listagem.
- Aguarde estado `READY` e confirme `LastCrawl.Status`.

### Athena não encontra tabela ou retorna duplicidade

- Verifique Glue Database e resultado do último crawler.
- Use os nomes efetivos de tabela do catálogo.
- Filtre `run_id` para evitar contar várias execuções.
- Confirme location S3 e partição `run_id`.

### Alarme de erros Lambda

- Consulte a função, logs e métricas `Errors`, `Invocations`, `Duration` e `Throttles`.
- Verifique a fila SQS e mensagens na DLQ/quarentena, se configurada.
- Confirme a política e assinatura SNS.
- Não apague mensagens ou objetos de evidência antes de registrar a falha.

## 8. Critérios de aceite por execução

- Glue Job `SUCCESS`.
- Relatório de qualidade `PASSED`.
- Taxas de correspondência dentro dos limites configurados.
- Saídas Gold com `run_id` esperado.
- Crawler Silver e Gold `SUCCEEDED`.
- Athena consulta a partição esperada.
- Métricas e alarmes verificados.
- Evidência da execução acrescentada sem sobrescrever histórico.

`PASSED` não comprova a completude das fontes oficiais. Mantenha visíveis as limitações de metas e dos microdados.

## 9. Encerramento e limpeza

Se a infraestrutura não for mais necessária, exporte primeiro as evidências. Depois confirme o bucket exato e sua propriedade, limpe todas as versões do bucket pelo script de purge e revise o plano de `terraform destroy`. A limpeza é destrutiva e não deve ser automatizada sem conferência humana.
