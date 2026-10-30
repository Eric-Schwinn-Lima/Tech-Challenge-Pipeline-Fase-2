# FinOps — estimativa baseada em medições

**Projeto:** Pipeline híbrida de dados para análise da alfabetização
**Região:** `us-east-2` (US East — Ohio)
**Última execução Glue usada como referência:** `20261029T231856Z_37704ef5`
**Data de referência dos registros:** outubro de 2026

> Esta estimativa separa consumo observado, preço público de referência e projeção hipotética. Não representa uma fatura final nem um valor total garantido. Preços podem variar por região, modalidade, nível gratuito, impostos e condições da conta. Validar os valores finais na AWS Pricing Calculator e no Cost Explorer.

## 1. Perfil observado da execução AWS

| Medição | Valor observado |
|---|---:|
| Job Glue | `tc-alfabetizacao-a9455abd-medallion-pipeline` |
| Versão do Glue | 4.0 |
| Tipo de worker | G.1X |
| Quantidade de workers | 2 |
| Duração da execução | 227,87475 segundos |
| Duração em horas | 0,06329854 h |
| Registros de alunos lidos | 1.048.575 |
| Linhas na fato municipal | 23.995 |
| Linhas em `mart_alfabetizacao_uf` | 145 |
| Linhas em `mart_evolucao_temporal` | 7 |
| Linhas em `reconciliacao_microdados` | 12.393 |
| Status do job | `SUCCESS` |
| Status das verificações de qualidade | `PASSED` |

A execução de referência foi manual. O agendamento diário está desabilitado (`enable_daily_schedule = false`). Portanto, o cenário mensal abaixo é uma projeção de capacidade, não o consumo operacional atual.

## 2. Cálculo verificável do AWS Glue ETL

A página pública do AWS Glue apresenta US$ 0,44 por DPU-hora como preço de referência para ETL e informa que a cobrança é proporcional ao uso, por segundo, com duração mínima de um minuto. O preço efetivo deve ser confirmado para `us-east-2`.

Para workers G.1X, cada worker equivale a 1 DPU. Assim:

```text
DPUs utilizadas = 2 workers × 1 DPU = 2 DPUs
Horas executadas = 227,87475 / 3.600
                 = 0,06329854 h

Custo estimado por execução =
    2 DPUs × 0,06329854 h × US$ 0,44
  = US$ 0,05570
```

### Cenário hipotético de execução diária

| Unidade | Quantidade | Preço de referência | Custo projetado |
|---|---:|---:|---:|
| Glue ETL, uma execução | 2 DPU × 0,06329854 h | US$ 0,44/DPU-h | US$ 0,05570 |
| Glue ETL, 30 execuções/mês | 60 DPU × 0,06329854 h | US$ 0,44/DPU-h | **US$ 1,67108/mês** |

Este valor cobre somente o tempo do job ETL. Não inclui crawlers, tentativas adicionais, execução de outros jobs, armazenamento, requisições S3 ou outros serviços.

## 3. Athena — consulta observada e projeção ilustrativa

Uma consulta de validação executada no Athena registrou 1.149 bytes escaneados e aproximadamente 493 ms de execução. A página oficial informa cobrança por volume escaneado, arredondamento por MB e mínimo de 10 MB por consulta; o preço de referência divulgado é US$ 5 por TB escaneado.

Como o volume observado está abaixo do mínimo faturável, a estimativa ilustrativa por consulta é:

```text
Volume faturável mínimo = 10 MB
Custo de referência = 10 MB / 1.000.000 MB × US$ 5/TB
                    ≈ US$ 0,00005 por consulta
```

| Cenário | Quantidade | Custo ilustrativo |
|---|---:|---:|
| Consulta observada (mínimo faturável) | 1 | US$ 0,00005 |
| 30 consultas semelhantes no mês | 30 | US$ 0,00150 |
| 100 consultas semelhantes no mês | 100 | US$ 0,00500 |

A simulação pressupõe que cada consulta permaneça no mínimo de 10 MB faturáveis. Consultas futuras que leiam mais dados terão custo maior. DDL, consultas com falha e outros detalhes de faturamento devem ser avaliados conforme a regra oficial vigente.

## 4. Serviços implantados e dados ainda necessários

Não se deve confundir número de mensagens, execuções ou objetos com unidades faturáveis. Para concluir uma projeção integral, precisamos extrair as unidades de consumo listadas abaixo.

| Serviço | Unidade faturável relevante | Dado disponível | O que falta para fechar a projeção |
|---|---|---|---|
| S3 Standard | GB-mês, PUT/COPY/POST/LIST, GET e transferência | Fontes e saídas armazenadas no bucket; versionamento habilitado | Tamanho total atual, versões não atuais, quantidade de requisições e retenção |
| Glue ETL | DPU-hora | 2 DPU por 227,87475 s na última execução | Frequência mensal efetiva e quantidade de novas tentativas |
| Glue Crawlers | DPU-hora | Dois crawlers concluíram com sucesso | Duração e DPUs efetivamente consumidas por crawler |
| Glue Data Catalog | Objetos de metadados e acessos faturáveis | 12 tabelas catalogadas | Volume de objetos/partições e eventual cobrança acima da franquia |
| Athena | TB escaneados por consulta | Uma consulta observada com 1.149 bytes escaneados | Quantidade mensal de consultas e bytes escaneados de cada uma |
| Lambda | Solicitações e GB-segundo | Dois eventos de teste: um válido e um inválido | Invocações totais, memória configurada e duração total faturável |
| SQS Standard | Requisições, em blocos de até 64 KB | Dois eventos de teste enviados ao fluxo | Solicitações API totais (send, receive, delete, retries e polling) |
| Step Functions Standard | Transições de estado | Uma execução `SUCCEEDED` | Número real de transições por execução, incluindo retries |
| CloudWatch | Métricas customizadas, alarmes, logs ingeridos/armazenados | Três nomes de métricas customizadas e um alarme Lambda | Quantidade faturável de métricas, volume de logs e retenção |
| SNS | Notificações/publicações e transferências aplicáveis | Tópico e assinatura de e-mail configurados | Quantidade mensal de publicações e notificações |

### Referências públicas de preço

- **Glue:** US$ 0,44 por DPU-hora como referência publicada; crawlers também são cobrados conforme DPU-hora.
- **Athena:** cobrança por dados escaneados; referência de US$ 5/TB e mínimo de 10 MB por consulta.
- **Lambda:** nível gratuito publicado de 1 milhão de solicitações e 400.000 GB-segundo/mês; após franquias, preço depende de arquitetura, região e memória.
- **SQS:** a página oficial informa 1 milhão de requisições gratuitas por mês; cada ação API conta como requisição e payloads maiores podem consumir múltiplos blocos.
- **Step Functions Standard:** cobrança por transições de estado; o nível gratuito publicado inclui 4.000 transições/mês. A página consultada apresenta US$ 0,000025 por transição em sua tabela/exemplo para N. Virginia; confirmar o preço específico de Ohio na calculadora.
- **S3:** armazenamento, solicitações e transferência são cobrados separadamente; o preço depende da classe, região, volume, tipo de requisição e retenção.
- **CloudWatch:** verificar preços de métricas customizadas, alarmes, logs ingeridos e armazenamento conforme a região.

## 5. Resultado do Cost Explorer observado

Na consulta histórica realizada para outubro de 2026 (1º a 29 de outubro, com a data final exclusiva em 30 de outubro), os valores apresentados estavam próximos de zero. Foram observados valores residuais muito pequenos em S3 e transferência de dados, enquanto diversos outros serviços apareciam como US$ 0,00.

Essa leitura deve ser apresentada como **consumo de baixo volume no período de teste**, não como custo total definitivo nem como garantia de custo zero. O Cost Explorer pode ter atraso de atualização e valores arredondados na visualização. Reconciliar com a fatura após o fechamento do período.

## 6. Controles FinOps implementados

| Controle | Configuração / evidência |
|---|---|
| AWS Budget | Orçamento diário de US$ 10 |
| Alerta do orçamento | Custo real em 85% do orçamento (US$ 8,50/dia) |
| CloudWatch Alarm | `tc-alfabetizacao-a9455abd-stream-lambda-errors` |
| Métrica do alarme | `Errors` da Lambda |
| Limite do alarme | 1 |
| Estado observado | `OK` no momento da verificação |
| Notificação | SNS com assinatura de e-mail confirmada |
| Métricas customizadas | `EventsIngested`, `InvalidEvents`, `EventLatencySeconds` |
| Agendamento diário | Desabilitado por padrão |

O orçamento de US$ 10/dia é um limite de alerta e governança, não uma previsão de gasto nem um custo fixo.

## 7. Cenário mensal de referência — não é a fatura total

Com os dados atualmente comprovados, só é possível calcular com segurança o subconjunto Glue ETL e um cenário ilustrativo de Athena:

| Componente | Cenário | Custo mensal calculável |
|---|---|---:|
| Glue ETL | 30 execuções iguais à execução de referência | US$ 1,67108 |
| Athena | 30 consultas semelhantes, mínimo 10 MB cada | US$ 0,00150 |
| **Subtotal conhecido modelado** | **Somente estes dois componentes** | **US$ 1,67258** |

**Não apresentar US$ 1,67258 como custo mensal total da arquitetura.** S3, crawlers, Lambda, SQS, Step Functions, CloudWatch e SNS ainda dependem da extração de quantidades adicionais. Também não se deve assumir que o nível gratuito se aplica integralmente: ele depende da elegibilidade e do consumo agregado da conta.

## 8. Comandos para coletar as métricas que faltam

Executar no PowerShell com perfil `AWS-tech-challenge-terraform` e região `us-east-2`:

### 8.1 Tamanho agregado do bucket

```powershell
aws s3 ls s3://tc-alfabetizacao-a9455abd-lake/ --recursive --summarize --profile AWS-tech-challenge-terraform --region us-east-2
```

Registre `Total Objects` e `Total Size`. Para bucket versionado, o tamanho de objetos atuais não inclui necessariamente todas as versões antigas.

### 8.2 Configuração de versionamento e lifecycle

```powershell
aws s3api get-bucket-versioning --bucket tc-alfabetizacao-a9455abd-lake --profile AWS-tech-challenge-terraform --region us-east-2

aws s3api get-bucket-lifecycle-configuration --bucket tc-alfabetizacao-a9455abd-lake --profile AWS-tech-challenge-terraform --region us-east-2
```

### 8.3 Configuração atual do Glue Job

```powershell
aws glue get-job --job-name tc-alfabetizacao-a9455abd-medallion-pipeline --profile AWS-tech-challenge-terraform --region us-east-2 --query "Job.{WorkerType:WorkerType,NumberOfWorkers:NumberOfWorkers,Timeout:Timeout,GlueVersion:GlueVersion}" --output table
```

### 8.4 Histórico e duração das execuções Glue

```powershell
aws glue get-job-runs --job-name tc-alfabetizacao-a9455abd-medallion-pipeline --max-results 20 --profile AWS-tech-challenge-terraform --region us-east-2 --query "JobRuns[].{RunId:Id,Started:StartedOn,Ended:CompletedOn,ExecutionTime:ExecutionTime,DPUSeconds:DPUSeconds,Status:JobRunState}" --output table
```

### 8.5 Contagem de execuções Step Functions

Use o ARN da máquina de estados do projeto:

```powershell
aws stepfunctions list-executions --state-machine-arn arn:aws:states:us-east-2:262325794007:stateMachine:tc-alfabetizacao-a9455abd-pipeline-orchestrator --max-results 50 --profile AWS-tech-challenge-terraform --region us-east-2 --output table
```

Para calcular transições, consultar o histórico de cada execução com `get-execution-history`.

### 8.6 Faturamento por serviço no Cost Explorer

O Cost Explorer deve ser consultado com período fechado e intervalo de datas explícito. O parâmetro `End` é exclusivo:

```powershell
aws ce get-cost-and-usage `
  --time-period Start=2026-10-01,End=2026-10-30 `
  --granularity MONTHLY `
  --metrics UnblendedCost UsageQuantity `
  --group-by Type=DIMENSION,Key=SERVICE `
  --profile AWS-tech-challenge `
  --region us-east-1 `
  --output json
```

Atualize as datas para o período efetivamente analisado quando repetir a medição. Cost Explorer é consultado em `us-east-1`, independentemente da região dos recursos.

## 9. Decisões de otimização

1. **Glue sob demanda:** o agendamento diário permanece desligado até que a frequência operacional seja justificada pelo negócio.
2. **Workers limitados:** a execução de referência utilizou dois workers G.1X; aumentar capacidade só após medir duração, memória e gargalos.
3. **Parquet/compaction:** manter formatos colunares e compressão quando efetivamente aplicados, para reduzir leitura e custo no Athena.
4. **Athena:** selecionar colunas necessárias e filtrar por `run_id` para evitar misturar execuções e ler partições desnecessárias.
5. **S3:** monitorar retenção, versionamento e tamanho de versões não atuais; lifecycle deve equilibrar custo e necessidade de histórico.
6. **Streaming:** SQS Standard e Lambda permitem processamento desacoplado em baixo volume; acompanhar polling, retries, DLQ (se adotada) e duplicidade.
7. **Alertas:** o orçamento diário e o CloudWatch Alarm devem ser revisados periodicamente para assegurar que notificações chegam aos responsáveis.

## 10. Conclusão

A arquitetura foi provisionada e executada em AWS, e existem controles operacionais de orçamento e alertas. O custo conhecido do Glue ETL pode ser estimado a partir da duração observada. O custo mensal total ainda deve ser fechado com métricas de armazenamento/versionamento S3, duração dos crawlers, volume de consultas Athena, uso Lambda/SQS, transições Step Functions e volume de logs/metricas CloudWatch.

A apresentação do FinOps deve manter explícita essa distinção entre **medição observada**, **projeção hipotética** e **componente ainda não quantificado**.

## Fontes oficiais de preço

- AWS Glue: https://aws.amazon.com/glue/pricing/
- Amazon Athena: https://aws.amazon.com/athena/pricing/
- Amazon S3: https://aws.amazon.com/s3/pricing/
- AWS Lambda: https://aws.amazon.com/lambda/pricing/
- Amazon SQS: https://aws.amazon.com/sqs/pricing/
- AWS Step Functions: https://aws.amazon.com/step-functions/pricing/
- AWS Pricing Calculator: https://calculator.aws/
