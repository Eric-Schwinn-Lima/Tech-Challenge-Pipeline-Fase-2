# Evidências visuais da AWS — Tech Challenge Pipeline de Alfabetização

Capturas do Console AWS fornecidas durante a validação de 02–03/11/2026. As imagens foram preservadas como evidências visuais; foram ocultados apenas dados de perfil, ARN/ID de conta e e-mail pessoal que não são necessários para a avaliação.

## Índice

| Nº | Evidência | O que demonstra |
|---:|---|---|
| 01 | [`01_step-functions_execution_succeeded.png`](01_orquestracao/01_step-functions_execution_succeeded.png) | Execução Step Functions `validacao-cloudwatch-20261102-204946`. O gráfico mostra os estados percorridos e o caminho de sucesso da execução. |
| 02 | [`02_glue_job_run_succeeded.png`](02_glue/02_glue_job_run_succeeded.png) | Histórico e detalhes do AWS Glue Job. A primeira execução listada é `SUCCEEDED`; a tela apresenta Glue 4.0, 2 DPUs e 0,175 DPU-hora. |
| 03 | [`03_glue_crawlers_succeeded.png`](02_glue/03_glue_crawlers_succeeded.png) | Console dos Crawlers Silver e Gold com estado Ready e último run Succeeded. |
| 04 | [`04_glue_data_catalog_current_inventory.png`](03_catalogo_athena/04_glue_data_catalog_current_inventory.png) | Inventário do Data Catalog na captura de 03/11/2026: a Console exibia 40 tabelas. É uma fotografia posterior e não deve ser confundida com a contagem histórica de 12 tabelas registrada em outra execução. |
| 05 | [`05_athena_exploratory_select.png`](03_catalogo_athena/05_athena_exploratory_select.png) | Consulta exploratória `SELECT ... LIMIT 10`, concluída no Athena. Esta captura demonstra acesso aos dados, mas não é a evidência visual do Quality Gate formal `PASS`; esse resultado permanece documentado nos registros de execução. |
| 06 | [`06_cloudwatch_log_groups_retention_30d.png`](04_cloudwatch/06_cloudwatch_log_groups_retention_30d.png) | Lista de grupos CloudWatch. Os grupos dedicados da Lambda e dos logs contínuos do Glue exibem retenção de 1 mês (30 dias); os grupos compartilhados aparecem sem expiração e foram mantidos inalterados. |
| 07 | [`07_cloudwatch_glue_log_events.png`](04_cloudwatch/07_cloudwatch_glue_log_events.png) | Eventos reais do fluxo de logs do Glue no grupo dedicado. A saída textual de `get-log-events` registrada na documentação também evidencia inicialização do GlueCloudwatchSink e obtenção de credenciais via Instance Profile. |
| 08 | [`08_lambda_sqs_monitoring.png`](05_streaming/08_lambda_sqs_monitoring.png) | Visão geral e monitoramento da função Lambda Stream Consumer com integração SQS e métricas de execução. O ARN da função foi ocultado por privacidade. |
| 09 | [`09_s3_medallion_structure.png`](06_s3/09_s3_medallion_structure.png) | Bucket S3 do projeto com as pastas Bronze, Silver e Gold, além de Athena results, scripts e observability. |
| 10 | [`10_aws_budget_daily_configuration.png`](07_finops/10_aws_budget_daily_configuration.png) | Detalhes do Daily Budget: orçamento diário de US$ 10, início em 20/10/2026 e destinatário verificado. O e-mail foi ocultado. A captura mostra “Filtros incluídos: Nenhum”; portanto, o orçamento não representa atribuição exclusiva ao Tech Challenge. |

## Notas de interpretação

- **Step Functions:** a execução documentada é `validacao-cloudwatch-20261102-204946`, com estado `SUCCEEDED`.
- **Glue:** a captura de Job Runs mostra execução bem-sucedida e 0,175 DPU-hora na execução selecionada. O campo `Log group name` exibido na tela de detalhes não substitui a evidência do grupo de logs contínuos personalizado; essa configuração é demonstrada nas telas do CloudWatch e no registro textual de `get-log-events`.
- **Data Catalog:** a Console exibia 40 tabelas na captura de 03/11/2026. A contagem histórica de 12 tabelas registrada em outra fotografia refere-se a momento/execução anterior. Não tratar os dois números como se fossem a mesma fotografia.
- **Athena:** a imagem incluída mostra uma consulta exploratória com `LIMIT 10`, não o Quality Gate formal. O resultado formal `PASS` está registrado nos documentos/artefatos de execução correspondentes.
- **CloudWatch:** os dois grupos dedicados apresentam retenção de 1 mês (30 dias). Grupos compartilhados do Glue aparecem sem expiração e não foram alterados por esta implementação.
- **Logs Glue:** a captura de eventos mostra mensagens do processamento. A confirmação específica de inicialização do `GlueCloudwatchSink` e obtenção de credenciais via Instance Profile também está registrada na evidência textual `get-log-events` documentada em `docs/evidencias_execucao.md`.
- **FinOps:** o Daily Budget mostrado é de escopo geral, pois a captura informa `Filtros incluídos: Nenhum`. Seu valor não deve ser apresentado como custo exclusivo do Tech Challenge. Atribuição financeira por projeto continua limitada pela falta de alocação consolidada por tag.
- As capturas não incluem uma tela formal de Cost Explorer atribuída ao projeto; os dados vistos no Cost Explorer eram agregados e não foram usados como custo exclusivo do pipeline.

## Relação com a documentação

- [`../evidencias_execucao.md`](../evidencias_execucao.md)
- [`../finops.md`](../finops.md)
- [`../../../README.md`](../../../README.md)
