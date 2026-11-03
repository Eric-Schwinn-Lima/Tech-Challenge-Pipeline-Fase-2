# FinOps — Tech Challenge Pipeline de Alfabetização

**Projeto:** AWS Tech Challenge — Pipeline de Alfabetização  

**Região de execução:** `us-east-2` (Ohio)  

**Período da fotografia de custos:** 1º a 29 de outubro de 2026  

**Data de consolidação:** 3 de novembro de 2026

**Atualização operacional:** evidências AWS até 2 de novembro de 2026  

**Repositório:** `Tech-Challenge-Pipeline-Fase-2`

> **Resumo executivo.** A arquitetura foi medida com baixo volume de processamento e armazenamento. O principal achado operacional foi o polling ocioso da fila SQS: em 29/10/2026, foram registrados 20.779 recebimentos vazios para apenas duas mensagens enviadas, recebidas e excluídas. Como o streaming será utilizado somente em testes e demonstrações, o Event Source Mapping foi desabilitado e o Terraform foi ajustado para mantê-lo desabilitado por padrão. Os valores do Cost Explorer apresentados nesta documentação estavam marcados como estimados e retornaram custo não alocado de US$ 0,00; isso não deve ser interpretado como garantia de custo zero ou como fatura final. Em 02/11/2026, também foram aplicadas retenções de 30 dias a grupos de logs dedicados da Lambda e do Glue, com validação real de entrega dos logs contínuos do Glue. Essa atividade e as novas execuções de novembro não fazem parte da fotografia de custos de outubro.

## 1. Objetivo e escopo

Esta documentação registra as medições, premissas, estimativas e decisões de otimização de custos (**FinOps**) da arquitetura. O objetivo é manter o ambiente econômico durante períodos sem testes, sem eliminar recursos necessários à demonstração do pipeline batch e streaming.

O escopo inclui S3, AWS Glue, Athena, Lambda, SQS, Step Functions, CloudWatch e SNS. A análise usa três tipos de evidência:

1. **Métricas operacionais:** CloudWatch, configurações dos recursos e histórico de execuções.

2. **Uso e custo agregados:** AWS Cost Explorer, agrupado por serviço, tipo de uso e tipo de registro.

3. **Referências de preço:** páginas oficiais de preços da AWS. São referências públicas, não substituem o preço efetivo da região/conta nem a fatura.

### 1.1 Limites da medição

- A consulta do Cost Explorer usou `Start=2026-10-01` e `End=2026-10-30`. A data final é exclusiva; portanto, a consulta cobre 1º a 29/10/2026.

- O Cost Explorer foi consultado na região de API `us-east-1`, conforme requerido pelo serviço. Os tipos de uso começavam por `USE2`, associados ao uso em Ohio (`us-east-2`).

- Os resultados do Cost Explorer tinham `Estimated = True` e `UnblendedCost = 0` nas linhas retornadas. São dados estimados e agregados à conta/região; não houve atribuição completa por recurso ou por tag do projeto.

- As métricas CloudWatch foram consultadas por recurso quando havia dimensão disponível. Nem toda métrica operacional representa diretamente uma unidade faturável.

- A fotografia do S3 é um inventário de objetos/versionamento em um instante, não uma média mensal de GB-mês.

- Os valores em USD calculados nesta documentação são aproximações de referência, não valores faturados.

## 2. Arquitetura e recursos considerados

| Serviço | Recurso do projeto | Função |
|---|---|---|
| Amazon S3 | `tc-alfabetizacao-a9455abd-lake` | Armazenamento das camadas do data lake e artefatos |
| AWS Glue | `tc-alfabetizacao-a9455abd-medallion-pipeline` | Processamento ETL batch |
| AWS Glue Crawlers | `tc-alfabetizacao-a9455abd-silver-crawler` e `tc-alfabetizacao-a9455abd-gold-crawler` | Descoberta/atualização de metadados |
| Glue Data Catalog | `tc_alfabetizacao_a9455abd_catalog` | Catálogo de tabelas |
| Amazon Athena | `tc-alfabetizacao-a9455abd-analytics` | Consultas SQL analíticas |
| AWS Lambda | `tc-alfabetizacao-a9455abd-stream-consumer` | Consumo de eventos streaming |
| Amazon SQS | `tc-alfabetizacao-a9455abd-events` | Fila de eventos streaming |
| AWS Step Functions | `tc-alfabetizacao-a9455abd-pipeline-orchestrator` | Orquestração da execução batch |
| Amazon SNS | `tc-alfabetizacao-a9455abd-alerts` | Notificações |
| Amazon CloudWatch | Métricas, logs e alarme da Lambda | Observabilidade e alertas |

Os recursos Terraform usam tags previstas no código, incluindo `Project`, `Environment` e `ManagedBy`. Entretanto, a extração apresentada do Cost Explorer não foi filtrada pela tag do projeto; portanto, seus números devem ser tratados como consumo agregado da conta na região, salvo quando identificados explicitamente por consulta de métrica do recurso.

## 3. Resumo das medições

### 3.1 Armazenamento S3

Inventário retornado por `list-object-versions` para o bucket do projeto:

| Métrica | Quantidade |
|---|---:|
| Versões atuais | 102 |
| Versões não atuais | 8 |
| Marcadores de exclusão | 0 |
| Armazenamento de versões atuais | 0,1968 GiB |
| Armazenamento de versões não atuais | 0,0596 GiB |
| **Armazenamento versionado total observado** | **0,2564 GiB** |

As versões não atuais representam aproximadamente 23,25% do total observado. O Terraform contém regra de expiração de versões não atuais após 90 dias e regra para abortar uploads multipart incompletos após 7 dias. A política reduz acúmulo de versões antigas, mas não elimina imediatamente versões não atuais.

**Limitação:** 0,2564 GiB é o tamanho observado no momento da consulta. O S3 cobra armazenamento de acordo com a quantidade armazenada ao longo do tempo, classe de armazenamento e demais componentes (solicitações, recuperação e transferência). Não há dados suficientes nesta coleta para calcular o custo mensal efetivo do bucket. Referência: [preços do Amazon S3](https://aws.amazon.com/s3/pricing/).

### 3.2 AWS Glue — execuções do ETL

Na fotografia histórica de 1º a 29/10/2026, foram avaliadas cinco execuções do job: duas concluídas com sucesso e três que falharam (incluindo as execuções de teste do Quality Gate). As execuções adicionais de 02/11/2026 estão separadas na seção 3.12 e não foram incorporadas aos totais históricos abaixo.

| Indicador | Resultado |
|---|---:|
| Execuções analisadas | 5 |
| Execuções `SUCCEEDED` | 2 |
| Execuções `FAILED` | 3 |
| DPU-segundos somados (`DPUSeconds`) | 1.987 |
| DPU-horas equivalentes | 0,551944 h |
| DPU-segundos das execuções bem-sucedidas | 1.166 |
| DPU-horas bem-sucedidas | 0,323889 h |
| DPU-segundos das execuções com falha | 821 |
| DPU-horas das execuções com falha | 0,228056 h |

A conversão usada é:

`DPU-horas = DPU-segundos / 3.600`

Como referência pública, a página de preços do AWS Glue apresenta US$ 0,44 por DPU-hora para ETL e crawlers em seus exemplos, observando que os preços podem variar por região. Aplicando apenas esse valor de referência ao `DPUSeconds` das cinco execuções:

| Grupo | DPU-horas | Custo de referência |
|---|---:|---:|
| Execuções bem-sucedidas | 0,323889 | US$ 0,14251 |
| Execuções com falha | 0,228056 | US$ 0,10034 |
| **Total das cinco execuções** | **0,551944** | **US$ 0,24286** |

Esse cálculo é uma estimativa baseada no consumo reportado nas execuções do Glue e no preço de referência. Não é uma reconciliação de fatura.

### 3.3 AWS Glue — Cost Explorer agregado

A consulta do Cost Explorer para o serviço AWS Glue retornou as seguintes quantidades por tipo de uso:

| Tipo de uso (`USE2`) | Quantidade | Unidade | Custo retornado |
|---|---:|---|---:|
| `Catalog-Request` | 1.454 | Requests | US$ 0,00 |
| `Catalog-Storage` | 45,665322 | Obj-Month | US$ 0,00 |
| `Crawler-DPU-Hour` | 0,643389 | DPU-Hour | US$ 0,00 |
| `ETL-DPU-Hour` | 0,751666 | DPU-Hour | US$ 0,00 |
| `ETL-DPU-Hour-Gen2` | 0,908611 | DPU-Hour | US$ 0,00 |
| `GlueInteractiveSession-DPU-Hour` | 6,136107 | DPU-Hour | US$ 0,00 |
| `GlueInteractiveSession-DPU-Hour-Gen2` | 2,144443 | DPU-Hour | US$ 0,00 |
| `DBrew-FreeSessions` | 1 | Sessions | US$ 0,00 |

Aplicando US$ 0,44/DPU-hora somente às categorias convencionais `ETL-DPU-Hour` e `Crawler-DPU-Hour`, obtém-se uma referência agregada de aproximadamente US$ 0,33073 para ETL e US$ 0,28309 para crawlers, totalizando US$ 0,61382. **Essas quantidades são agregadas à conta/região e não estão comprovadamente atribuídas apenas a este projeto.** Não somar esse valor ao cálculo por `DPUSeconds` da seção anterior: são bases de medição diferentes e com escopos distintos.

O Cost Explorer também retornou consumo Gen2 e Interactive Sessions. Não foi calculado preço para essas categorias usando a taxa convencional de US$ 0,44/DPU-hora, pois isso exigiria identificar o mecanismo tarifário e o escopo de uso específico.

O AWS Glue informa que o primeiro milhão de objetos do Data Catalog e o primeiro milhão de acessos são gratuitos conforme as condições da página oficial. A consulta registrou 1.454 requisições e 45,665322 Obj-Month, mas a elegibilidade real depende do uso agregado da conta. Referência: [preços do AWS Glue](https://aws.amazon.com/glue/pricing/).

### 3.4 Crawlers

| Crawler | Último runtime observado | Runtime mediano observado | Tabelas atualizadas |
|---|---:|---:|---:|
| Silver | 38,371 s | 37,4625 s | 8 |
| Gold | 37,461 s | 36,869 s | 4 |

O runtime, isoladamente, não determina o custo: é necessário considerar DPU consumida, duração faturável, região e regras de cobrança. Para a análise monetária, foram mantidas as quantidades agregadas do Cost Explorer descritas acima.

### 3.5 Amazon Athena

Foram observadas 13 consultas concluídas com sucesso no workgroup do projeto. Todas foram identificadas como DML. O total de bytes verificados foi:

- Bytes processados: **1.886.683 bytes**.

- Equivalente aproximado: **1,80 MiB**.

- Consultas concluídas: **13**.

Como referência, a página oficial de preços do Athena informa preço por volume de dados verificados, arredondamento para MB e mínimo de 10 MB por consulta SQL faturável; a página de preços deve ser consultada para o preço aplicável à região. Usando a referência pública de US$ 5/TB e assumindo que as 13 consultas estejam sujeitas ao mínimo de 10 MB, a estimativa ilustrativa é:

`13 consultas × 10 MB × US$ 5 / 1.000.000 MB = US$ 0,00065`

O valor é aproximado e não representa cobrança observada no Cost Explorer. Consultas DDL e consultas com falha têm tratamento de cobrança diferente. Referência: [preços do Amazon Athena](https://aws.amazon.com/athena/pricing/).

### 3.6 AWS Lambda

Configuração observada da função `tc-alfabetizacao-a9455abd-stream-consumer`:

| Configuração/métrica | Resultado |
|---|---:|
| Runtime | Python 3.12 |
| Arquitetura | x86_64 |
| Memória | 256 MB |
| Timeout | 60 s |
| Invocações observadas no CloudWatch | 2 |
| Duração acumulada observada | 613,05 ms |
| Erros | 0 |
| Throttles | 0 |

Estimativa simples de computação a partir da memória configurada e da duração acumulada do CloudWatch:

`0,25 GB × 0,61305 s = 0,1532625 GB-segundo`

O Cost Explorer, por sua vez, retornou `USE2-Lambda-GB-Second = 0,2565` e `USE2-Request = 2`, com custo retornado de US$ 0,00 e `Estimated = True`. A diferença entre 0,1533 GB-segundo calculado com a duração do CloudWatch e 0,2565 GB-segundo do Cost Explorer não foi reconciliada. O Cost Explorer foi consultado por serviço e região, não por identificador da função; portanto, seu valor pode incluir outros usos da conta. Manter as duas medidas separadas até uma extração por recurso/relatório de faturamento.

A página de preços do Lambda apresenta franquia mensal de 1 milhão de solicitações e 400.000 GB-segundos, sujeita às condições e agregação aplicáveis à conta. Referência: [preços do AWS Lambda](https://aws.amazon.com/lambda/pricing/).

### 3.7 Amazon SQS — fila streaming

Configuração observada da fila `tc-alfabetizacao-a9455abd-events`:

| Atributo | Valor |
|---|---:|
| Long polling (`ReceiveMessageWaitTimeSeconds`) | 20 s |
| Visibility timeout | 360 s |
| Retenção de mensagens | 86.400 s (24 h) |
| Batch size no Event Source Mapping | 10 |
| Maximum batching window | 0 s |
| Estado final do Event Source Mapping | `Disabled` |

Métricas CloudWatch do período consultado:

| Métrica | Total |
|---|---:|
| `NumberOfMessagesSent` | 2 |
| `NumberOfMessagesReceived` | 2 |
| `NumberOfMessagesDeleted` | 2 |
| `NumberOfEmptyReceives` | **20.779** |

Os 20.779 recebimentos vazios foram registrados nas 24 faixas horárias de 29/10/2026 (horário de Brasília). O valor por hora ficou próximo de 900 em quase todas as horas. Isso é compatível com polling enquanto a fila estava vazia, mas a métrica da fila não identifica sozinha qual consumidor originou cada chamada.

A fila foi criada em 29/10/2026 às 00:53:28 (horário de Brasília) e o Event Source Mapping teve `LastModified` em 29/10/2026 às 00:54:03, diferença de aproximadamente 35 segundos. A sequência temporal reforça a hipótese de atividade relacionada à integração Lambda–SQS, mas não prova exclusividade causal.

O Cost Explorer retornou `USE2-Requests-Tier1 = 19.304 Requests`, `USE2-DataTransfer-In-Bytes = 0,0000003 GB`, custo US$ 0,00 e `Estimated = True`. O total de 19.304 requisições não coincide com os 20.779 recebimentos vazios do CloudWatch. As consultas têm escopos/contabilizações distintos; a diferença permanece como ponto de reconciliação. Não somar os números nem usá-los como se fossem a mesma métrica.

A página oficial do SQS informa 1 milhão de solicitações gratuitas por mês para todos os clientes. Isso é agregado conforme as regras do nível gratuito e não deve ser interpretado como franquia exclusiva desta fila. Referência: [preços do Amazon SQS](https://aws.amazon.com/sqs/pricing/).

### 3.8 AWS Step Functions

Na fotografia de custos de 1º a 29/10/2026, o Cost Explorer registrou `USE2-StateTransition = 4 StateTransitions`, além de pequenas quantidades de transferência de dados (`0,0000948943 GB` de entrada e `0,00001609 GB` de saída), com custo retornado de US$ 0,00 e `Estimated = True`. Esse dado não inclui as execuções de Step Functions realizadas em 02/11/2026, descritas na seção 3.12.

A máquina de estados do projeto é do tipo Standard. A página oficial de preços indica 4.000 transições gratuitas por mês para workflows Standard. A franquia é agregada à conta e inclui transições de todas as máquinas e novas tentativas. Referência: [preços do AWS Step Functions](https://aws.amazon.com/step-functions/pricing/).

### 3.9 Amazon CloudWatch

O Cost Explorer retornou as seguintes quantidades agregadas:

| Tipo de uso | Quantidade | Unidade |
|---|---:|---|
| `CW:AlarmMonitorUsage` | 0,6421370544 | Alarms |
| `CW:MetricMonitorUsage` | 1,2741935437 | Metrics |
| `CW:Requests` | 1.061 | Requests |
| `DataProcessing-Bytes` | 0,0257854903 | GB |
| `TimedStorage-ByteHrs` | 0,0013838884 | GB-Month |
| `VendedLog-Bytes` | 0,00000085 | GB |

Todas as linhas da fotografia de 1º a 29/10/2026 retornaram custo de US$ 0,00 e `Estimated = True`. As quantidades de métricas e alarmes são unidades de uso faturável agregadas ao longo do período, não contagens simples de recursos configurados. Os grupos de logs dedicados configurados em 02/11/2026 e seus eventos posteriores não estão incluídos nessa fotografia.

A página de preços do CloudWatch apresenta franquias para determinados usos, como métricas personalizadas, solicitações de API, alarmes padrão e logs. A aplicação exata depende do tipo de métrica/alarme, região, volume e agregação da conta. Referência: [preços do Amazon CloudWatch](https://aws.amazon.com/cloudwatch/pricing/).

### 3.10 Amazon SNS

O Cost Explorer retornou:

| Tipo de uso | Quantidade | Unidade |
|---|---:|---|
| `USE2-Requests-Tier1` | 124 | Requests |
| `USE2-DeliveryAttempts-SMTP` | 3 | Notifications |
| Transferência de entrada | 0,00000301 | GB |
| Transferência de saída | 0,0000081889 | GB |

Todas as linhas retornaram custo de US$ 0,00 e `Estimated = True`. A página oficial do SNS apresenta franquias mensais de requisições e entregas por e-mail, sujeitas às condições e agregação da conta. Referência: [preços do Amazon SNS](https://aws.amazon.com/sns/pricing/) e [FAQ de faturamento do SNS](https://aws.amazon.com/sns/faqs/).

### 3.11 CloudWatch Logs — retenção e validação operacional em 02/11/2026

Em 02/11/2026, o Terraform passou a gerenciar dois grupos de logs dedicados ao projeto, ambos com retenção de 30 dias:

| Grupo de logs | Finalidade | Retenção configurada |
|---|---|---:|
| `/aws/lambda/tc-alfabetizacao-a9455abd-stream-consumer` | Logs da função Lambda | 30 dias |
| `/aws-glue/jobs/logs-v2/tc-alfabetizacao-a9455abd` | Logs contínuos do job Glue do projeto | 30 dias |

A configuração do job Glue mantém `--enable-continuous-cloudwatch-log = true` e define `--continuous-log-logGroup = /aws-glue/jobs/logs-v2/tc-alfabetizacao-a9455abd`.

O plano revisado apresentou `1 to add, 2 to change, 0 to destroy`; a aplicação concluiu com `1 added, 2 changed, 0 destroyed`. O grupo Lambda existente foi importado e atualizado *in-place*. O grupo exclusivo do Glue foi criado e o job foi atualizado *in-place*. Ambos os recursos declararam `skip_destroy = true`.

A validação na AWS confirmou `retentionInDays = 30` nos dois grupos. Depois da execução `validacao-cloudwatch-20261102-204946` (`SUCCEEDED`), o CloudWatch apresentou um fluxo no grupo exclusivo do Glue:

- Grupo: `/aws-glue/jobs/logs-v2/tc-alfabetizacao-a9455abd`.
- Log stream: `jr_16518f4f47e0e9ae0b60e1a199f8a5fe362d3671325c8fa60e8c3d9abf689cd8-1`.
- Job Run ID registrado no fluxo: `jr_16518f4f47e0e9ae0b60e1a199f8a5fe362d3671325c8fa60e8c3d9abf689cd8`.
- `get-log-events` retornou mensagens de inicialização do Spark e do `GlueCloudwatchSink`, incluindo a obtenção de credenciais pelo Instance Profile e a identificação do job/job run.

Na resposta de `describe-log-streams`, `storedBytes` apareceu como zero; entretanto, `get-log-events` retornou mensagens para o fluxo. O valor de `storedBytes` não foi utilizado como medição do volume de logs.

Os grupos padrão compartilhados do Glue — por exemplo, `/aws-glue/jobs/error`, `/aws-glue/jobs/output`, `/aws-glue/jobs/logs-v2` sem o sufixo do projeto e `/aws-glue/crawlers` — não foram alterados. A nova configuração direciona os logs contínuos do job ao grupo exclusivo; não migra os logs históricos dos grupos compartilhados nem altera automaticamente todos os demais tipos de log.

`skip_destroy = true` protege o grupo contra exclusão pelo Terraform quando o recurso é removido da configuração. Isso não desativa a expiração dos eventos: a retenção de 30 dias continua aplicável. Os custos de ingestão e armazenamento desses logs após 02/11 ainda não foram reconciliados com uma fatura ou com alocação por tag; a fotografia do Cost Explorer de outubro não os inclui.

### 3.12 Execuções AWS posteriores à fotografia de custos de outubro

As execuções abaixo ocorreram em 02/11/2026 e devem ser tratadas como atividade posterior ao período de Cost Explorer de 1º a 29/10. Não foram incorporadas aos totais históricos das seções 3.2, 3.8 ou 4.

| Execução Step Functions | Início (Brasília) | Fim (Brasília) | Estado da orquestração |
|---|---|---|---|
| `validacao-medallion-20261102-164701` | 16:47:02 | 16:57:20 | `FAILED` |
| `validacao-final-20261102-170352` | 17:03:53 | 17:15:03 | `SUCCEEDED` |
| `validacao-cloudwatch-20261102-204946` | 20:49:47 | 21:00:06 | `SUCCEEDED` |

Na primeira tentativa, o Glue concluiu com `SUCCEEDED`, duração de 321 segundos e `642 DPUSeconds`, mas a Step Function falhou numa etapa posterior de recuperação do resultado por JSONPath. Na execução seguinte, o Glue concluiu com `SUCCEEDED`, duração de 371 segundos e `742 DPUSeconds`; a orquestração terminou `SUCCEEDED` e a validação Athena retornou `PASS` para oito tabelas, com 187.200 linhas somadas e uma linha em `mart_streaming_eventos` para o `run_id` analisado.

Para as duas execuções Glue com `DPUSeconds` disponíveis:

| Execução Glue | DPU-segundos | DPU-horas |
|---|---:|---:|
| `jr_c1c9b53c0bdba3f3807ecfa75304ed3d1d6fff5020bf02b5cd359f59f67ac919` | 642 | 0,178333 |
| `jr_72e1d75c775ccf77fc2675b98ffc00f12b5c3652f41068de5119dbe88e444fd7` | 742 | 0,206111 |
| **Total observado** | **1.384** | **0,384444** |

Aplicando somente como referência o valor de US$ 0,44/DPU-hora utilizado nesta documentação, as duas execuções equivalem a aproximadamente **US$ 0,16916**. Trata-se de estimativa baseada em `DPUSeconds`, não de custo faturado ou atribuição final do Cost Explorer.

A execução `validacao-cloudwatch-20261102-204946` também terminou `SUCCEEDED` e comprovou a chegada de logs contínuos ao grupo dedicado. O `DPUSeconds` dessa execução não foi incluído no cálculo acima porque não foi coletado no material de evidência utilizado nesta atualização. Também não foram calculados custos adicionais de crawlers, Athena, Step Functions ou CloudWatch para essas execuções. Não extrapolar os valores conhecidos para o custo total do pipeline.

## 4. Consolidação de custos e interpretação

### 4.1 Cost Explorer — visão agregada

Para Lambda, SQS, Step Functions, CloudWatch, SNS e Glue, as linhas exibidas pelo Cost Explorer retornaram `UnblendedCost = 0`. Todas estavam marcadas como estimadas. Os resultados incluem quantidades de uso, mas não identificam isoladamente os recursos do projeto.

**Conclusão correta:** no momento da extração, o Cost Explorer não apresentou custo líquido/unblended diferente de zero nas linhas analisadas. **Conclusão incorreta:** “a arquitetura custa zero” ou “não haverá cobrança”. A conta pode ter franquias, créditos, descontos, defasagem de atualização ou outros itens não abrangidos pela consulta; é necessário confrontar a fatura final e os créditos/ajustes de billing.

### 4.2 Estimativas de referência que podem ser reproduzidas

| Item | Base | Estimativa |
|---|---|---:|
| Glue ETL — cinco execuções | 0,551944 DPU-h × US$ 0,44 | US$ 0,24286 |
| Glue ETL — Cost Explorer (conta/região) | 0,751666 DPU-h × US$ 0,44 | US$ 0,33073 |
| Glue Crawlers — Cost Explorer (conta/região) | 0,643389 DPU-h × US$ 0,44 | US$ 0,28309 |
| Athena — amostra de 13 consultas | 13 × 10 MB × US$ 5/TB | US$ 0,00065 |

**Atenção:** as duas linhas de Glue baseadas no Cost Explorer não são exclusivas do projeto; não devem ser somadas à estimativa por execução nem apresentadas como uma fatura do projeto. A linha de Athena é uma estimativa pelo mínimo por consulta, não um valor efetivamente faturado.

## 5. Controles de orçamento

Foi configurado um AWS Budget diário de **US$ 10,00**, com alerta de gasto real em **85% (US$ 8,50)**.

- O alerta é um mecanismo de monitoramento/notificação.

- O orçamento não é um limite rígido de consumo e não bloqueia automaticamente os recursos quando o valor é atingido.

- Acompanhar também custos previstos e custos reais no Billing/Cost Explorer.

- Validar o orçamento após alterações de arquitetura, aumento do volume de dados ou mudança do padrão de execução.

## 6. Decisão operacional para streaming

O streaming será utilizado somente durante testes e demonstrações. A infraestrutura (fila SQS e função Lambda) permanece provisionada, mas o Event Source Mapping fica desabilitado por padrão para impedir que a integração continue consumindo a fila quando não há necessidade operacional.

### 6.1 Estado implantado e versionado

- Event Source Mapping UUID: `38bbba4a-0154-4d4a-93f1-0d6e56b10a1d`.

- Estado final confirmado na AWS: `Disabled`.

- Código Terraform: `cloud/terraform/main.tf` com `enabled = false`.

- Commit GitHub: `db5df8e` — `finops: disable streaming consumer by default`.

- A alteração foi sincronizada no clone de validação; branch `main` alinhada com `origin/main`.

- Validação local: `terraform fmt -check`, `terraform validate` e suíte Python com **9 testes aprovados**.

### 6.2 Habilitar para um teste

Execute no PowerShell com perfil e região corretos:

```powershell

$awsProfile = "AWS-tech-challenge-terraform"

$awsRegion = "us-east-2"

$mappingUuid = "38bbba4a-0154-4d4a-93f1-0d6e56b10a1d"

aws lambda update-event-source-mapping `

    --uuid $mappingUuid `

    --enabled `

    --profile $awsProfile `

    --region $awsRegion

```

A atualização pode retornar inicialmente `Enabling`. Aguarde e confirme `Enabled`:

```powershell

aws lambda get-event-source-mapping `

    --uuid $mappingUuid `

    --profile $awsProfile `

    --region $awsRegion `

    --query State `

    --output text

```

Envie os eventos de teste somente depois de confirmar `Enabled`.

### 6.3 Desabilitar após o teste

```powershell

aws lambda update-event-source-mapping `

    --uuid $mappingUuid `

    --no-enabled `

    --profile $awsProfile `

    --region $awsRegion

```

A atualização pode retornar inicialmente `Disabling`. Confirme até o estado ser `Disabled`:

```powershell

do {

    $state = aws lambda get-event-source-mapping `

        --uuid $mappingUuid `

        --profile $awsProfile `

        --region $awsRegion `

        --query State `

        --output text

    Write-Host "Estado atual: $state"

    if ($state -ne "Disabled") {

        Start-Sleep -Seconds 3

    }

} while ($state -ne "Disabled")

```

### 6.4 Atenção à retenção da fila

A retenção SQS está configurada em 86.400 segundos (24 horas). Enquanto o Event Source Mapping estiver desabilitado, mensagens ainda podem ser enviadas à fila por produtores autorizados, mas não serão consumidas por essa integração. Mensagens que ultrapassarem o período de retenção expiram. Antes de uma demonstração, verifique a quantidade de mensagens visíveis e a idade da mensagem mais antiga; não dependa de eventos antigos acumulados.

### 6.5 Terraform e estado

Em 02/11/2026, foi feita uma comparação dos três arquivos de estado disponíveis: duas cópias no OneDrive e o arquivo do clone `C:\Users\erics\tech_challenge_clone_validacao\cloud\terraform\terraform.tfstate`. Os três apresentavam o mesmo `lineage` e os mesmos 31 recursos principais. As cópias do OneDrive estavam no `serial 32`; o state do clone estava no `serial 42`. O `terraform plan -refresh-only` executado no clone retornou `No changes`, indicando que os objetos remotos correspondiam à configuração e ao state naquele momento.

A partir dessa validação, o clone passou a ser o diretório de trabalho utilizado para a mudança de CloudWatch Logs. Os dois arquivos do OneDrive não foram alterados e devem ser tratados como cópias históricas, não como estados concorrentes para novos `apply`. Foi criado backup do state antes da alteração e outro após a importação do grupo Lambda, no diretório local `%LOCALAPPDATA%\TechChallenge\terraform-state-backups`.

O grupo Lambda existente `/aws/lambda/tc-alfabetizacao-a9455abd-stream-consumer` foi importado para `aws_cloudwatch_log_group.lambda_stream_consumer`. O plano salvo e revisado para a política de retenção apresentou `1 to add, 2 to change, 0 to destroy`. A aplicação terminou com `1 added, 2 changed, 0 destroyed`: criou `aws_cloudwatch_log_group.glue_continuous`, atualizou in-place o grupo Lambda e atualizou in-place `aws_glue_job.pipeline` para apontar para o grupo de logs contínuos dedicado.

A configuração aplicada declara `retention_in_days = 30` e `skip_destroy = true` nos dois grupos de logs. O job Glue mantém `--enable-continuous-cloudwatch-log = true` e usa `--continuous-log-logGroup` para o grupo `/aws-glue/jobs/logs-v2/tc-alfabetizacao-a9455abd`. A verificação na AWS confirmou 30 dias de retenção nos dois grupos e a execução de 02/11 comprovou a chegada de mensagens no grupo Glue.

Não copie, publique ou descarte `terraform.tfstate`, `terraform.tfstate.backup`, backups locais ou o diretório `.terraform` sem procedimento de gestão de estado. Esses arquivos podem conter informações sensíveis. Para futuras mudanças, utilizar somente o diretório/state escolhido como gerenciamento, revisar o plano completo e conferir que não existam exclusões ou substituições inesperadas antes de qualquer `terraform apply`.

## 7. Recomendações e backlog FinOps

| Prioridade | Ação | Motivo / critério de conclusão |
|---|---|---|
| Alta | Manter Event Source Mapping desabilitado fora dos testes | Reduz polling desnecessário; confirmar estado `Disabled` após cada demonstração |
| Alta | Acompanhar a métrica `NumberOfEmptyReceives` durante os testes | Distinguir volume de eventos de chamadas vazias e medir evolução |
| Alta | Reconciliar SQS CloudWatch × Cost Explorer | CloudWatch indicou 20.779 recebimentos vazios, mas Cost Explorer mostrou 19.304 requisições; investigar escopo, período e contabilização |
| Média | Monitorar ingestão e retenção dos grupos de logs dedicados | Conferir retenção de 30 dias, fluxo de logs contínuos do Glue e volume faturável após novas execuções; não alterar os grupos compartilhados sem avaliar outros jobs/crawlers |
| Média | Ativar e usar tags de alocação de custos | A consulta atual é agregada à conta; habilitar `Project` em Cost Allocation Tags, aguardar disponibilidade e filtrar relatórios futuros |
| Média | Extrair custos S3 por uso e classe de armazenamento | O inventário de 0,2564 GiB é instantâneo; falta GB-mês e solicitações por classe |
| Média | Reconciliar GB-segundos da Lambda | CloudWatch derivou ~0,1533 GB-s; Cost Explorer agregou 0,2565 GB-s |
| Média | Revisar artefatos locais antes de aplicar Terraform | O plano original indicou diferenças no código Lambda e Glue que não faziam parte do ajuste FinOps |
| Baixa | Revisar retenção e lifecycle do S3 periodicamente | Confirmar se 90 dias para versões não atuais atende à recuperação e aos custos |

## 8. Rotina mensal de acompanhamento

1. No início do mês, registrar período, região e data/hora de extração.

2. No Cost Explorer, filtrar cada serviço, agrupar por `USAGE_TYPE` e `RECORD_TYPE`, e manter a coluna `Estimated` junto dos valores.

3. Para serviços com recursos identificáveis, coletar métricas CloudWatch por dimensão do recurso e comparar com o Cost Explorer sem presumir equivalência entre métricas operacionais e unidades faturáveis.

4. Registrar Glue `DPUSeconds`, estado e duração de cada execução; separar execuções bem-sucedidas e falhas.

5. Registrar Athena bytes verificados e quantidade de consultas; priorizar Parquet, compressão, filtros e seleção explícita de colunas.

6. Registrar inventário S3 e versões não atuais, sem confundir tamanho instantâneo com GB-mês.

7. Confirmar o estado `Disabled` do Event Source Mapping após cada demonstração e verificar mensagens visíveis/idade na fila.

8. Conferir os grupos de logs Lambda e Glue: retenção de 30 dias, `skip_destroy = true` e chegada de logs contínuos ao grupo dedicado do Glue; acompanhar ingestão/armazenamento sem inferir custo somente por `storedBytes`.

9. Comparar valores estimados com a fatura final e registrar créditos, descontos, impostos e eventuais cobranças compartilhadas.

### 8.1 Consulta Cost Explorer — modelo

A API do Cost Explorer deve ser chamada em `us-east-1`; a região de uso aparece nos códigos dos tipos de uso. Exemplo de intervalo mensal (a data `End` é exclusiva):

```powershell

aws ce get-cost-and-usage `

    --time-period Start=2026-10-01,End=2026-11-01 `

    --granularity MONTHLY `

    --metrics UnblendedCost UsageQuantity `

    --group-by Type=DIMENSION,Key=USAGE_TYPE Type=DIMENSION,Key=RECORD_TYPE `

    --profile AWS-tech-challenge `

    --region us-east-1 `

    --output json

```

Para atribuição ao projeto, filtrar pela tag de custo `Project` somente depois de ativá-la no Billing e verificar a disponibilidade dos dados. Enquanto isso, declarar expressamente que os totais por serviço são agregados à conta.

## 9. Conclusão

A otimização implementada combina duas decisões operacionais: manter a fila SQS e a Lambda prontas, mas deixar o Event Source Mapping desabilitado fora de testes; e aplicar retenção de 30 dias a grupos de logs dedicados da Lambda e dos logs contínuos do Glue, com gerenciamento via Terraform. O polling observado em 29/10/2026 foi desproporcional ao volume de mensagens e justificou a primeira decisão. A segunda foi aplicada em 02/11 e validada por uma execução real que gerou mensagens no grupo exclusivo do Glue.

Os dados sustentam a conclusão de baixo volume observado e oportunidades concretas de reduzir polling ocioso e controlar o acúmulo de logs. Eles ainda não sustentam um custo mensal total definitivo para o projeto: a fotografia do Cost Explorer de outubro estava estimada e agregada à conta; as execuções e os logs de novembro ainda não foram reconciliados com uma fatura nem alocados integralmente por tag/recurso. A evolução da análise deve priorizar atribuição por projeto, reconciliação das métricas SQS/Lambda e CloudWatch, acompanhamento da retenção e conferência com a fatura final.

## 10. Referências oficiais AWS

- [AWS Glue — preços](https://aws.amazon.com/glue/pricing/)

- [Amazon S3 — preços](https://aws.amazon.com/s3/pricing/)

- [Amazon Athena — preços](https://aws.amazon.com/athena/pricing/)

- [AWS Lambda — preços](https://aws.amazon.com/lambda/pricing/)

- [Amazon SQS — preços](https://aws.amazon.com/sqs/pricing/)

- [AWS Step Functions — preços](https://aws.amazon.com/step-functions/pricing/)

- [Amazon CloudWatch — preços](https://aws.amazon.com/cloudwatch/pricing/)

- [Amazon SNS — preços](https://aws.amazon.com/sns/pricing/)

- [AWS Cost Explorer API — GetCostAndUsage](https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/API_GetCostAndUsage.html)

- [Lambda — SQS event source mapping](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html)

---

**Controle de versão:** documento atualizado em 03/11/2026. A fotografia de custos do Cost Explorer continua restrita a 01–29/10/2026; foram acrescentadas evidências operacionais, execuções e configuração de retenção aplicadas/validadas em 02/11/2026. Atualizar as medições, premissas e custos quando houver nova extração do Cost Explorer ou fatura consolidada.