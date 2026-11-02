# Registro consolidado de evidências de execução

Este documento separa a execução local da execução na AWS. Os resultados não devem ser somados ou tratados como uma mesma execução.

## 1. Execução AWS integrada — 02/11/2026

| Campo | Resultado |
|---|---|
| Data | 02/11/2026 |
| Região | `us-east-2` |
| Step Functions execution name | `validacao-final-20261102-170353` |
| Estado da Step Functions | `SUCCEEDED` |
| Início / término | 17:03:53.726 / 17:15:03.980 (UTC−03) |
| Duração total | Aproximadamente 11 min 10 s |
| `run_id` | `d5095134-779d-453d-b55a-c67a020ca4f3` |
| Glue Job Run ID | `jr_72e1d75c775ccf77fc2675b98ffc00f12b5c3652f41068de5119dbe88e444fd7` |
| Estado do Glue | `SUCCEEDED` |
| Duração do Glue | 371 s |
| `DPUSeconds` do Glue | 742 |
| Crawler Silver / Gold | `SUCCEEDED` / `SUCCEEDED` |
| Reparo Athena — fato municipal | `SUCCEEDED`; Query ID `400eb989-9ddf-4844-9e3c-a73272249ac1` |
| Reparo Athena — mart UF | `SUCCEEDED`; Query ID `cf993e91-5c07-4eaa-90d6-da0ab3eada84` |
| Validação Athena | `SUCCEEDED`; Query ID `94a4e66c-83d8-459a-baef-41777bfd36ce` |
| Quality Gate | `PASS` |
| Tabelas Gold verificadas | 8 |
| Total de registros reportado | 187.200 |
| Registros de streaming simulados | 1 |

A máquina de estados executou o Glue de forma síncrona, iniciou e aguardou os crawlers Silver e Gold, executou os dois reparos de partição no Athena, consultou a validação Gold e avaliou o Quality Gate. O estado final `SUCCEEDED` confirma a conclusão de todas essas etapas na execução identificada acima.

As oito tabelas Gold verificadas foram `fato_alfabetizacao_municipio`, `mart_alfabetizacao_uf`, `mart_evolucao_temporal`, `reconciliacao_microdados`, `mart_perfil_proficiencia`, `mart_acompanhamento_metas`, `mart_trajetoria_metas` e `mart_streaming_eventos`. O total de 187.200 é o total consolidado da consulta de qualidade; não foi capturada nesta execução uma distribuição individual por tabela. O único registro de streaming foi simulado e não constitui indicador oficial de alfabetização.

### Tentativa integrada anterior e correção

A tentativa anterior usou o `run_id` `117b9877-6796-4039-b56e-6ab35e6974f0`. O Glue terminou `SUCCEEDED` em 321 s, com 642 DPU-segundos, e a validação Athena retornou `PASS` para oito tabelas e 187.200 registros. A Step Functions, porém, terminou `FAILED` na etapa `GetGoldValidationResults`, porque o JSONPath tentava ler `$.gold_validation.QueryExecutionId`. O caminho correto é `$.gold_validation.QueryExecution.QueryExecutionId`. Após a correção, a execução registrada na seção 1 terminou `SUCCEEDED`.

## 2. Execução AWS histórica — 29/10/2026

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
| `mart_streaming_eventos` | **Não produzida** |

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

### Step Functions

A execução `validacao-pipeline-20261029201808` terminou `SUCCEEDED` em 29/10/2026. A máquina orquestra o Glue Job de forma síncrona e não inicia os crawlers; os crawlers foram executados separadamente depois da execução.

### Streaming AWS

- A fila SQS acionou a Lambda e os objetos foram gravados em `bronze/streaming/`.
- Foram testadas duas mensagens: uma carga malformada e uma mensagem JSON válida. A mensagem válida registrou latência observada de aproximadamente **2,180166 s**.
- A mensagem de demonstração não possuía o esquema analítico completo esperado pelo Glue (por exemplo, ano, série, rede e taxa). O Glue preservou/encaminhou o conteúdo para quarentena em vez de gerar uma tabela analítica.
- Portanto, não há evidência de uma `mart_streaming_eventos` AWS nessa execução. Não apresente o evento operacional como indicador oficial de alfabetização.

### Monitoramento e FinOps

- Alarme CloudWatch da Lambda em estado `OK`, com ação SNS habilitada; limiar `Errors >= 1` em janela de cinco minutos.
- Assinatura de e-mail SNS confirmada.
- Métricas customizadas no namespace `AlfabetizacaoPipeline`: `EventsIngested`, `InvalidEvents` e `EventLatencySeconds`.
- AWS Budget diário de **US$ 10**, notificação de custo real em **85% (US$ 8,50)**.
- Cost Explorer consultado para 01–29/10/2026 (consulta com fim exclusivo em 30/10). Os valores observados estavam próximos de zero: S3 aproximadamente US$ 0,0000000007 e Data Transfer aproximadamente -US$ 0,0000000015; os demais serviços consultados apresentaram valores 0,00 no relatório naquele momento. Isso não prova custo final zero: Cost Explorer/Billing pode ter atraso e ajustes.

## 3. Execução local

A execução local documentada é `20261029T003707Z_763b68ad`, `SUCCESS`, duração 30,811 s. Foram avaliadas 61 regras de qualidade, sem falhas bloqueantes. Houve cinco saídas Gold locais, incluindo duas linhas em `mart_streaming_eventos` produzidas a partir de eventos simulados. Consulte [`evidencias_execucao_local.md`](evidencias_execucao_local.md) para os detalhes. Esses resultados não são saídas AWS.

## 4. Limitações transversais

1. `alunos.csv` tem 1.048.575 linhas; é um indício forte de possível truncamento, não prova. A reconciliação por microdados é exploratória até confirmação da completude na fonte.
2. A tabela de metas cobre anos-alvo 2024–2030; não foi encontrada meta municipal 2023.
3. Em 2024, na série 2/rede municipal, 96 municípios não possuíam linha na fonte de metas; 120 possuíam linha, mas valor vazio também no arquivo bruto.
4. `PASSED` significa aprovação das regras automatizadas implementadas, não completude de todos os registros das fontes.
5. A tabela AWS de streaming não foi produzida com o esquema dos eventos de teste utilizado.

## 5. Evidências que ainda devem ser capturadas para a apresentação

- Prints do Console Glue com Job Run ID, configuração e estado.
- Capturas dos crawlers e das 8 tabelas Gold verificadas na execução integrada; registre separadamente as tabelas Silver exibidas no Data Catalog.
- Print do Athena com consulta filtrada por `run_id` e bytes escaneados.
- Print do CloudWatch Alarm e das métricas customizadas.
- Print do AWS Budget e da notificação configurada, sem expor endereço de e-mail.
- Vídeo executivo de até cinco minutos, caso ainda não tenha sido gravado.

Não inclua Access Keys, tokens, e-mail privado, IDs de conta ou dados pessoais nas capturas. Os arquivos JSON desta pasta são resumos estruturados; não substituem exportações brutas de API/CloudWatch.
