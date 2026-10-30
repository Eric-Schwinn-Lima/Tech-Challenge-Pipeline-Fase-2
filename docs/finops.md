# FinOps — medição, controles e projeção de custos

## 1. Objetivos e princípios

- Medir o consumo observado, separando resultados locais de custos AWS.
- Evitar downloads e reprocessamentos desnecessários de arquivos oficiais.
- Preservar Bronze com `run_id`, mas controlar retenção e versões não correntes.
- Usar Parquet + Snappy em Silver/Gold na AWS para reduzir leitura e armazenamento em comparação com formatos textuais, sem assumir economia sem medir.
- Usar SQS Standard + Lambda orientada a eventos, sem consumidor sempre ativo.
- Manter Glue enxuto: Glue 4.0, dois workers G.1X, timeout de 30 minutos. Aumentar recursos somente com evidência de gargalo.
- Desabilitar o agendamento diário por padrão (`enable_daily_schedule = false`).
- Projetar consultas Athena com filtros de partição (`run_id`) e seleção apenas das colunas necessárias.
- Desligar recursos de demonstração quando não forem mais necessários, depois de exportar evidências e avaliar a retenção.

## 2. Medições locais — não são custo AWS

Execução local `20261029T003707Z_763b68ad`, registrada em `evidencias_execucao_local.md`:

| Métrica | Valor |
|---|---:|
| Registros nas seis entidades obrigatórias | 1.110.375 |
| Linhas de alunos | 1.048.575 |
| Dicionário auxiliar | 27 |
| Bytes copiados para Bronze local | 162.132.540 |
| Eventos simulados recebidos | 4 |
| Eventos encaminhados à Gold local | 2 |
| Eventos em quarentena | 1 |
| Duplicados removidos | 1 |
| Verificações de qualidade | 61 |
| Falhas bloqueantes locais | 0 |
| Linhas nas cinco saídas Gold locais | 36.542 |

Na execução local documentada, PyArrow não estava instalado e a saída usou fallback `.csv.gz`; portanto, não registre essa execução local como Parquet. O job AWS Glue produziu Parquet Snappy.

## 3. Medição AWS observada

Execução mais recente registrada em 29/10/2026:

| Métrica | Valor observado |
|---|---:|
| Glue Run ID | `20261029T231856Z_37704ef5` |
| Estado | `SUCCESS` |
| Quality status | `PASSED` |
| Duração do job | 227,87475 s (aprox. 3,80 min) |
| Workers | 2 × G.1X |
| Fato municipal | 23.995 linhas |
| Mart por UF | 145 linhas |
| Evolução temporal | 7 linhas |
| Reconciliação de microdados | 12.393 linhas |
| Tabelas no Data Catalog | 12 (8 Silver + 4 Gold) |

Uma consulta Athena à visão por UF teve duração observada de aproximadamente 493 ms e 1.149 bytes escaneados. Esse dado descreve aquela consulta específica; não é o consumo de todas as consultas. Sempre capture `Data scanned` de cada consulta usada como evidência.

## 4. Controles implementados

### AWS Budget

- Tipo: custo real (ACTUAL).
- Periodicidade: diária.
- Limite configurado: **US$ 10 por dia**.
- Notificação: **85%**, equivalente a **US$ 8,50**, por e-mail.

O Budget é um mecanismo de alerta, **não um bloqueio automático de gastos** nem uma previsão de custo mensal. A projeção mensal deve usar o padrão real de execução e recursos persistentes.

### CloudWatch e SNS

- Alarme da Lambda: métrica `AWS/Lambda – Errors`, soma, período de 300 s, uma avaliação, limiar maior ou igual a 1.
- Estado observado: `OK`; ações habilitadas e direcionadas ao tópico SNS.
- Assinatura de e-mail SNS: confirmada.
- Métricas customizadas no namespace `AlfabetizacaoPipeline`: `EventsIngested`, `InvalidEvents`, `EventLatencySeconds`.

### S3

- Bucket versionado, criptografado com SSE-S3 e bloqueio de acesso público.
- Abort de multipart upload incompleto após sete dias.
- Expiração de versões não correntes após 90 dias.
- A listagem de objetos atuais com `aws s3 ls --recursive --summarize` não contabiliza todas as versões não correntes; a cobrança real deve ser conferida no Cost Explorer/S3 Storage Lens.

## 5. Cost Explorer — fotografia observada

Consulta realizada em 29/10/2026 para o período de **01/10/2026 a 29/10/2026**. A consulta utilizou 30/10/2026 como data final exclusiva.

| Serviço/linha | Valor observado no relatório |
|---|---:|
| S3 | aproximadamente US$ 0,0000000007 |
| Data Transfer | aproximadamente US$ -0,0000000015 |
| Demais serviços consultados (Glue, Lambda, Athena, SQS, SNS, Step Functions, CloudWatch, KMS, Secrets Manager e CloudFormation) | US$ 0,00 no relatório consultado |

Os valores de S3 e transferência são residuais e próximos de zero; a linha negativa de transferência pode refletir ajuste/crédito. **Não declarar custo final zero.** Cost Explorer e Billing podem apresentar atraso, ajustes posteriores e arredondamentos. Consulte novamente antes da entrega final e após a permanência de recursos na conta.

## 6. Projeção mensal — o que pode e o que não pode ser afirmado

Com os registros disponíveis, não há base suficiente para uma estimativa mensal confiável em dólares por serviço. O orçamento de US$ 10/dia é um limite de notificação e não pode ser apresentado como custo esperado.

Para calcular projeção, registre por serviço:

| Serviço | Métrica necessária | Fonte de medição |
|---|---|---|
| S3 | GB-mês, número de objetos, versões, PUT/GET/LIST | S3 Storage Lens, métricas e Cost Explorer |
| Glue | worker type, workers, duração faturável por execução | Glue Job Run e Cost Explorer |
| SQS | requisições, tamanho médio de payload, mensagens | Métricas SQS/CloudWatch e Cost Explorer |
| Lambda | invocações, duração e memória | CloudWatch `Invocations`/`Duration` |
| Step Functions | execuções e transições | Histórico de execuções e tipo de workflow |
| Athena | bytes escaneados por consulta | Detalhes da query e Cost Explorer |
| CloudWatch | ingestão/armazenamento de logs, métricas e alarmes | CloudWatch/Cost Explorer |
| SNS | notificações/publicações entregues | Métricas SNS/Cost Explorer |

A projeção é: uso medido por execução × frequência mensal planejada, somado ao armazenamento que permanece entre execuções e aos serviços continuamente ativos. Consulte os preços oficiais na região `us-east-2` na data da estimativa. Não preencha preço unitário ou custo mensal por aproximação não verificada.

## 7. Consultas Athena econômicas

- Filtre sempre `run_id` nas tabelas Gold.
- Filtre ano, UF ou município conforme a pergunta.
- Selecione colunas necessárias, evite `SELECT *`.
- Registre o volume escaneado.
- Não trate várias execuções como uma única série sem escolher uma regra explícita.

## 8. Retenção e desligamento

Antes de destruir a infraestrutura:

1. Exporte resultados, logs, métricas, relatórios de qualidade e prints exigidos.
2. Confirme que o bucket é exclusivamente do projeto.
3. Remova versões S3 não correntes com o script de purge do projeto, após revisar o nome e a região.
4. Execute `terraform destroy` somente depois de revisar o plano destrutivo.
5. Confira Billing/Cost Explorer depois da remoção, lembrando do atraso de atualização.

O script `scripts/purge_versioned_bucket.py` é destrutivo e não deve ser executado em bucket que contenha dados de outra finalidade.

## 9. Preços oficiais

- [AWS Pricing Calculator](https://calculator.aws/)
- [Amazon S3](https://aws.amazon.com/s3/pricing/)
- [AWS Glue](https://aws.amazon.com/glue/pricing/)
- [Amazon Athena](https://aws.amazon.com/athena/pricing/)
- [Amazon SQS](https://aws.amazon.com/sqs/pricing/)
- [AWS Lambda](https://aws.amazon.com/lambda/pricing/)
- [AWS Step Functions](https://aws.amazon.com/step-functions/pricing/)
- [Amazon CloudWatch](https://aws.amazon.com/cloudwatch/pricing/)
- [Amazon SNS](https://aws.amazon.com/sns/pricing/)

Sempre registre a data de consulta, a região e as premissas da projeção.
