# Evidência AWS — bloqueio do Quality Gate

**Projeto:** Pipeline híbrida para análise da alfabetização
**Serviço:** AWS Glue / PySpark
**Região:** `us-east-2`
**Data do teste:** 30/10/2026 (UTC)

## 1. Objetivo

Comprovar que uma violação de integridade na chave natural da tabela `municipio` é identificada pelo AWS Glue e impede a publicação das camadas Silver e Gold.

## 2. Preparação do cenário

Foi criada uma cópia temporária das sete fontes canônicas. Na cópia de `municipio.csv`, foi duplicada uma linha com chave natural completa:

- `ano`
- `id_municipio`
- `serie`
- `rede`

O arquivo preparado continha 23.996 linhas e uma duplicidade na chave natural.

A fonte original local e os prefixos de produção não foram modificados.

Prefixo temporário utilizado:

`s3://tc-alfabetizacao-a9455abd-lake/bronze/quality-gate-tests/20261030T032109Z-c5693189/source/`

## 3. Identificadores da execução

| Campo | Valor |
|---|---|
| Glue Job | `tc-alfabetizacao-a9455abd-medallion-pipeline` |
| Glue JobRunId | `jr_59f157b16876a5d794504c72f8b7e5ae34f361779a7639232c24082e3a6d846d` |
| Run ID interno | `20261030T032201Z_ed47f526` |
| Estado final do Glue | `FAILED` |
| Regra bloqueante | `duplicate_natural_keys:municipio` |
| Resultado da regra | `FAIL` |
| Duplicidades detectadas | `1` |

## 4. Evidências persistidas no S3

Relatório de qualidade:

`s3://tc-alfabetizacao-a9455abd-lake/observability/quality/run_id=20261030T032201Z_ed47f526.json`

Métricas da execução:

`s3://tc-alfabetizacao-a9455abd-lake/observability/metrics/run_id=20261030T032201Z_ed47f526.json`

Arquivos complementares preservados neste repositório:

- `quality_gate_failed.json`
- `quality_gate_metrics.json`
- `quality_gate_glue_job_run.json`

## 5. Verificação de publicação

| Camada | Objetos publicados |
|---|---:|
| Silver de teste | 0 |
| Gold de teste | 0 |

A falha ocorreu antes da publicação Silver e, consequentemente, antes da publicação Gold.

## 6. Limpeza dos dados temporários

Após preservar as evidências, foram removidas permanentemente as versões dos objetos e os marcadores de exclusão dos quatro prefixos temporários utilizados nos testes.

A verificação posterior com `list-object-versions` confirmou:

| Prefixo de teste | Versões | Marcadores |
|---|---:|---:|
| `031208Z-51ae0c7b` | 0 | 0 |
| `031441Z-ef772359` | 0 | 0 |
| `031815Z-c2b8ff07` | 0 | 0 |
| `032109Z-c5693189` | 0 | 0 |

Os relatórios de qualidade e métricas foram preservados em `observability`. Os dados de produção não foram utilizados como destino de saída do teste.

## 7. Conclusão

O teste comprova que, diante de uma duplicidade na chave natural de `municipio`, o AWS Glue:

1. Detecta a violação pela regra bloqueante.
2. Registra a falha no relatório de qualidade.
3. Registra as métricas da execução com status `FAILED`.
4. Encerra o job com estado `FAILED`.
5. Não publica objetos nos prefixos de saída Silver e Gold do teste.

Este cenário demonstra o funcionamento do Quality Gate na AWS, separado da execução bem-sucedida de produção.

## 8. Segunda execução independente — 30/10/2026

Uma segunda execução do teste foi realizada com o script
`scripts/test_glue_quality_gate.ps1`, utilizando uma nova cópia
temporária das sete fontes.

| Campo | Resultado |
|---|---|
| Glue JobRunId | `jr_a6929c6ebebf3532ccde25591a7d7868b29798272182c52cb7f4a5437efadc99` |
| Run ID interno | `20261030T034815Z_f2b72df9` |
| Estado final | `FAILED` |
| Regra bloqueante | `duplicate_natural_keys:municipio` |
| Resultado da regra | `FAIL` |
| Duplicidades detectadas | 1 |
| Objetos Silver publicados | 0 |
| Objetos Gold publicados | 0 |

Relatório de qualidade:

`s3://tc-alfabetizacao-a9455abd-lake/observability/quality/run_id=20261030T034815Z_f2b72df9.json`

Métricas:

`s3://tc-alfabetizacao-a9455abd-lake/observability/metrics/run_id=20261030T034815Z_f2b72df9.json`

As três evidências JSON foram preservadas em `docs/evidencias/aws/`,
com o identificador desta execução nos nomes dos arquivos.

A fonte temporária foi removida permanentemente do S3. A consulta
posterior confirmou zero versões e zero marcadores de exclusão no
prefixo utilizado. A cópia local temporária também foi removida.

Esta segunda execução confirma a repetibilidade do bloqueio de
qualidade, sem publicar dados nos prefixos Silver e Gold de teste.

## 8. Segunda execução independente — 30/10/2026

Uma segunda execução do teste foi realizada com o script
`scripts/test_glue_quality_gate.ps1`, utilizando uma nova cópia
temporária das sete fontes.

| Campo | Resultado |
|---|---|
| Glue JobRunId | `jr_a6929c6ebebf3532ccde25591a7d7868b29798272182c52cb7f4a5437efadc99` |
| Run ID interno | `20261030T034815Z_f2b72df9` |
| Estado final | `FAILED` |
| Regra bloqueante | `duplicate_natural_keys:municipio` |
| Resultado da regra | `FAIL` |
| Duplicidades detectadas | 1 |
| Objetos Silver publicados | 0 |
| Objetos Gold publicados | 0 |

Relatório de qualidade:

`s3://tc-alfabetizacao-a9455abd-lake/observability/quality/run_id=20261030T034815Z_f2b72df9.json`

Métricas:

`s3://tc-alfabetizacao-a9455abd-lake/observability/metrics/run_id=20261030T034815Z_f2b72df9.json`

As três evidências JSON foram preservadas em `docs/evidencias/aws/`,
com o identificador desta execução nos nomes dos arquivos.

A fonte temporária foi removida permanentemente do S3. A consulta
posterior confirmou zero versões e zero marcadores de exclusão no
prefixo utilizado. A cópia local temporária também foi removida.

Esta segunda execução confirma a repetibilidade do bloqueio de
qualidade, sem publicar dados nos prefixos Silver e Gold de teste.
