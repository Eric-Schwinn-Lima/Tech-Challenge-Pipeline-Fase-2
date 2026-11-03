# Registro consolidado de evidências de execução

Este documento separa a execução local das execuções realizadas na AWS. Os resultados de períodos e ambientes diferentes não devem ser somados nem tratados como uma mesma execução. A fotografia histórica de 29/10/2026 permanece separada das execuções orquestradas em 02/11/2026.

## 1. Execução AWS — fotografia histórica de 29/10/2026

| Campo | Resultado |
|---|---|
| Data de referência | 29/10/2026 |
| Região | `us-east-2` |
| Glue Job Run ID | `20261029T231856Z_37704ef5` |
| Estado do Glue | `SUCCESS` |
| Quality status | `PASSED` |
| Duração | 227,87475 s (aprox. 227,87 s) |
| Configuração do Glue | Glue 4.0; 2 workers G.1X; timeout 30 min |

### Linhas lidas

| Fonte | Registros |
|---|---:|
| `uf` | 145 |
| `municipio` | 23.995 |
| `meta_alfabetizacao_brasil` | 7 |
| `meta_alfabetizacao_uf` | 189 |
| `meta_alfabetizacao_municipio` | 37.464 |
| `alunos` | 1.048.575 |
| `dicionario` | 27 |

As seis entidades obrigatórias totalizam 1.110.375 linhas; com o dicionário auxiliar, 1.110.402.

### Saídas Gold

| Tabela | Registros |
|---|---:|
| `fato_alfabetizacao_municipio` | 23.995 |
| `mart_alfabetizacao_uf` | 145 |
| `mart_evolucao_temporal` | 7 |
| `reconciliacao_microdados` | 12.393 |
| `mart_streaming_eventos` | Não produzida nesta execução |

### Qualidade e catálogo

- Taxa de correspondência `alunos → municipio`: **99,822809%**.
- Taxa de correspondência `municipio → uf`: **98,639456%**.
- Quality status do job: `PASSED`.
- Crawler Silver: `SUCCEEDED`.
- Crawler Gold: `SUCCEEDED`.
- Glue Data Catalog: 12 tabelas (8 Silver e 4 Gold).
- Uma consulta Athena filtrada pelo `run_id` retornou 23.995 linhas em `fato_alfabetizacao_municipio` e 5.550 municípios distintos.
- Na mesma consulta de qualidade: 18.763 linhas com meta municipal nula e zero linhas com taxa de alfabetização nula.
- Distribuição de `status_meta_municipio`: `Atingida` 2.788; `abaixo` 2.444; `SEM_META` 5.664; `NAO_COMPARAVEL` 13.099.

O total dos quatro estados é 23.995. `SEM_META` e `NAO_COMPARAVEL` são condições distintas e não significam necessariamente falha no processamento.

### Step Functions — histórico de 29/10

A execução `validacao-pipeline-20261029201808` terminou `SUCCEEDED` em 29/10/2026. Naquele fluxo histórico, a máquina orquestrava o Glue Job de forma síncrona; os crawlers eram executados separadamente depois da execução. A arquitetura foi ampliada posteriormente: as execuções de 02/11 descritas na seção 2 passaram a incluir crawlers e consultas de validação na orquestração.

### Streaming AWS — histórico de 29/10

- A fila SQS acionou a Lambda e os objetos foram gravados em `bronze/streaming/`.
- Foram testadas duas mensagens: uma carga malformada e uma mensagem JSON válida. A mensagem válida registrou latência observada de aproximadamente **2,180166 s**.
- A mensagem de demonstração não possuía o esquema analítico completo esperado pelo Glue (por exemplo, ano, série, rede e taxa). O Glue preservou/encaminhou o conteúdo para quarentena em vez de gerar uma tabela analítica.
- Portanto, não há evidência de `mart_streaming_eventos` nesta execução AWS de 29/10. Não apresente o evento operacional como indicador oficial de alfabetização.

### Monitoramento e FinOps — fotografia de outubro

- Alarme CloudWatch da Lambda em estado `OK`, com ação SNS habilitada; limiar `Errors >= 1` em janela de cinco minutos.
- Assinatura de e-mail SNS confirmada.
- Métricas customizadas no namespace `AlfabetizacaoPipeline`: `EventsIngested`, `InvalidEvents` e `EventLatencySeconds`.
- AWS Budget diário de **US$ 10**, notificação de custo real em **85% (US$ 8,50)**.
- Cost Explorer consultado para 01–29/10/2026 (consulta com fim exclusivo em 30/10). Os valores observados estavam próximos de zero: S3 aproximadamente US$ 0,0000000007 e Data Transfer aproximadamente -US$ 0,0000000015; os demais serviços consultados apresentaram valores 0,00 no relatório naquele momento. Isso não prova custo final zero: Cost Explorer/Billing pode ter atraso e ajustes.

## 2. Execuções AWS — orquestração e validação de 02/11/2026

As execuções abaixo ocorreram depois da ampliação da máquina de estados. Os horários são locais de Brasília (`-03:00`). São execuções AWS distintas da fotografia histórica de 29/10.

### 2.1 Tentativa com falha na orquestração — 16:47

| Campo | Resultado |
|---|---|
| Step Functions execution | `validacao-medallion-20261102-164701` |
| Início | 02/11/2026 16:47:02 |
| Fim | 02/11/2026 16:57:20 |
| Estado da Step Function | `FAILED` |
| Glue Job Run ID | `jr_c1c9b53c0bdba3f3807ecfa75304ed3d1d6fff5020bf02b5cd359f59f67ac919` |
| Estado do Glue | `SUCCEEDED` |
| Duração do Glue | 321 s |
| DPU-segundos do Glue | 642 |

O processamento Glue terminou com sucesso, mas a execução da Step Function falhou em uma etapa posterior de recuperação do resultado por JSONPath, após a consulta Athena ter sido concluída. Portanto, não classificar essa tentativa como uma execução de orquestração ponta a ponta bem-sucedida.

### 2.2 Execução de validação concluída — 17:03

| Campo | Resultado |
|---|---|
| Step Functions execution | `validacao-final-20261102-170352` |
| Início | 02/11/2026 17:03:53 |
| Fim | 02/11/2026 17:15:03 |
| Estado da Step Function | `SUCCEEDED` |
| `run_id` do pipeline | `d5095134-779d-453d-b55a-c67a020ca4f3` |
| Glue Job Run ID | `jr_72e1d75c775ccf77fc2675b98ffc00f12b5c3652f41068de5119dbe88e444fd7` |
| Estado do Glue | `SUCCEEDED` |
| Duração do Glue | 371 s |
| DPU-segundos do Glue | 742 |

A execução consultou oito tabelas no Athena e retornou:

| Indicador | Resultado |
|---|---:|
| Quality status | `PASS` |
| Tabelas verificadas | 8 |
| Total de linhas retornadas pela validação | 187.200 |
| Linhas em `mart_streaming_eventos` para o `run_id` | 1 |

A validação exigiu linhas positivas nas sete tabelas diferentes de `mart_streaming_eventos`. O registro de streaming encontrado é evidência técnica de um evento processado, não um indicador oficial de alfabetização.

### 2.3 Execução de validação do CloudWatch Logs — 20:49

| Campo | Resultado |
|---|---|
| Step Functions execution | `validacao-cloudwatch-20261102-204946` |
| Início | 02/11/2026 20:49:47 |
| Fim | 02/11/2026 21:00:06 |
| Estado da Step Function | `SUCCEEDED` |
| Glue Job Run ID identificado no log stream | `jr_16518f4f47e0e9ae0b60e1a199f8a5fe362d3671325c8fa60e8c3d9abf689cd8` |

Essa execução foi realizada depois da atualização do job Glue para direcionar os logs contínuos ao grupo exclusivo do projeto. O CloudWatch retornou um fluxo de logs para o grupo:

`/aws-glue/jobs/logs-v2/tc-alfabetizacao-a9455abd`

O `get-log-events` retornou mensagens reais da execução, incluindo:

- Inicialização do componente `GlueCloudwatchSink`.
- Obtenção de credenciais por meio do Instance Profile.
- Identificação do job `tc-alfabetizacao-a9455abd-medallion-pipeline` e do respectivo `jobRunId`.
- Mensagens de inicialização e conexão dos executores Spark.

O campo `storedBytes` retornado na consulta de metadados do fluxo apareceu como zero, embora `get-log-events` tenha retornado mensagens. Por isso, esse campo não foi usado como medição do volume de logs; a evidência de entrega é a recuperação das mensagens pelo `get-log-events`.

## 3. Retenção e gerenciamento de CloudWatch Logs — 02/11/2026

A retenção dos grupos de logs dedicados ao projeto passou a ser gerenciada pelo Terraform.

| Grupo de logs | Finalidade | Retenção |
|---|---|---:|
| `/aws/lambda/tc-alfabetizacao-a9455abd-stream-consumer` | Logs da função Lambda | 30 dias |
| `/aws-glue/jobs/logs-v2/tc-alfabetizacao-a9455abd` | Logs contínuos do job Glue do projeto | 30 dias |

Configuração do Glue:

- `--enable-continuous-cloudwatch-log = true`
- `--continuous-log-logGroup = /aws-glue/jobs/logs-v2/tc-alfabetizacao-a9455abd`

O plano Terraform revisado registrou `1 to add, 2 to change, 0 to destroy`. A aplicação terminou com `1 added, 2 changed, 0 destroyed`:

- O grupo Lambda existente foi importado para o Terraform e atualizado *in-place*; não foi recriado.
- O grupo dedicado aos logs contínuos do Glue foi criado.
- O job Glue foi atualizado *in-place* para utilizar o grupo dedicado.
- `skip_destroy = true` foi definido nos dois recursos de grupo de logs.

Os grupos padrão compartilhados do Glue — como `/aws-glue/jobs/error`, `/aws-glue/jobs/output`, `/aws-glue/jobs/logs-v2` sem o sufixo do projeto e `/aws-glue/crawlers` — não foram alterados por essa mudança.

**Limite importante:** `skip_destroy = true` protege o grupo contra exclusão pelo Terraform quando o recurso é removido da configuração; não impede a expiração automática de eventos pela política de retenção de 30 dias. A configuração do grupo dedicado aplica-se aos logs contínuos direcionados pelo argumento do job, não representa uma migração dos logs históricos dos grupos compartilhados.

## 4. Execução local

A execução local documentada é `20261029T003707Z_763b68ad`, `SUCCESS`, duração 30,811 s. Foram avaliadas 61 regras de qualidade, sem falhas bloqueantes. Houve cinco saídas Gold locais, incluindo duas linhas em `mart_streaming_eventos` produzidas a partir de eventos simulados. Consulte [`evidencias_execucao_local.md`](evidencias_execucao_local.md) para os detalhes. Esses resultados não são saídas AWS.

## 5. Limitações transversais

1. `alunos.csv` tem 1.048.575 linhas; é um indício forte de possível truncamento, não prova. A reconciliação por microdados é exploratória até confirmação da completude na fonte.
2. A tabela de metas cobre anos-alvo 2024–2030; não foi encontrada meta municipal 2023.
3. Em 2024, na série 2/rede municipal, 96 municípios não possuíam linha na fonte de metas; 120 possuíam linha, mas valor vazio também no arquivo bruto.
4. `PASSED`/`PASS` significa aprovação das regras automatizadas implementadas, não completude de todos os registros das fontes.
5. A ausência de `mart_streaming_eventos` na execução AWS de 29/10 não deve ser generalizada para todas as execuções: na validação AWS de 02/11 às 17:03 foi retornada uma linha para o `run_id` daquela execução.
6. A validação do grupo CloudWatch confirma que mensagens foram recuperadas de um fluxo dedicado de logs contínuos do Glue; não constitui, por si só, uma medição de custo nem de volume faturável.

## 6. Evidências visuais e pendências para a apresentação

As capturas de tela do Console AWS estão organizadas em [`evidencias/aws/README.md`](evidencias/aws/README.md). O índice descreve o que cada imagem comprova e suas limitações de interpretação. O pacote inclui evidências de Step Functions, Glue Job, Crawlers, Data Catalog, consulta exploratória no Athena, CloudWatch Logs, Lambda/SQS, S3 e AWS Budgets.

**Limitações das capturas incluídas:**

- A captura do Athena mostra uma consulta exploratória `SELECT ... LIMIT 10`; ela não deve ser apresentada como captura do Quality Gate formal `PASS`. O resultado formal está nos registros textuais/estruturados da execução.
- A captura atual do Data Catalog mostra 40 tabelas na Console em 03/11/2026. A contagem histórica de 12 tabelas refere-se a uma fotografia anterior e não deve ser comparada como se fosse o mesmo instante.
- A captura do Daily Budget mostra `Filtros incluídos: Nenhum`; é evidência de configuração orçamentária de escopo geral, não de custo atribuído exclusivamente ao Tech Challenge.
- A captura da Lambda mostra a visão de monitoramento e a integração com SQS; o estado `Disabled` do Event Source Mapping deve ser demonstrado por consulta/tela específica caso seja exigido como evidência visual.

**Capturas complementares recomendadas, caso haja tempo:**

- Athena com a consulta de validação formal filtrada pelo `run_id`, exibindo o resultado `PASS`.
- CloudWatch Alarm e métricas customizadas no namespace `AlfabetizacaoPipeline`.
- Event Source Mapping da Lambda com estado `Disabled`.
- Vídeo executivo de até cinco minutos, caso exigido pela avaliação.

As imagens incluídas ocultam o perfil da Console, o ARN que expõe o ID da conta e o endereço de e-mail. Não inclua Access Keys, tokens, e-mails privados, IDs de conta ou dados pessoais em novas capturas. Os arquivos JSON desta pasta são resumos estruturados; não substituem exportações brutas de API/CloudWatch.
---

**Controle de versão:** documento atualizado em 03/11/2026 com evidências locais e AWS fornecidas até 03/11/2026. A fotografia de custos de 01–29/10/2026 permanece histórica; as novas execuções de 02/11 e a validação do CloudWatch estão identificadas separadamente.
