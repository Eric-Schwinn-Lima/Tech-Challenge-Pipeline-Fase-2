# Documentação do projeto

Este diretório reúne a documentação técnica, operacional, de dados, evidências de execução e controles FinOps do pipeline de alfabetização.

## Índice

| Arquivo | Conteúdo |
|---|---|
| [`architecture.md`](architecture.md) | Arquitetura, fluxo batch/streaming, responsabilidades e decisões técnicas |
| [`deploy_aws.md`](deploy_aws.md) | Pré-requisitos, autenticação, Terraform, carga, execução, catálogo e limpeza |
| [`dicionario_dados.md`](dicionario_dados.md) | Fontes, granularidade, chaves, campos canônicos e códigos oficiais |
| [`evidencias_execucao.md`](evidencias_execucao.md) | Registro consolidado das execuções local e AWS, limitações e evidências pendentes |
| [`evidencias_execucao_local.md`](evidencias_execucao_local.md) | Detalhe da execução local com fontes reais e eventos simulados |
| [`finops.md`](finops.md) | Medições observadas, orçamento, alertas, custo e limites da projeção |
| [`runbook.md`](runbook.md) | Operação, diagnóstico, reexecução, validação e desligamento seguro |
| [`evidencias/`](evidencias/) | Resumos JSON/JSONL legíveis por máquina; não substituem os logs brutos da AWS |

## Convenções importantes

- Região AWS usada na implantação documentada: `us-east-2`.
- Arquitetura de mensageria implantada: **Amazon SQS Standard → AWS Lambda → Amazon S3**. Kinesis não faz parte da implantação atual.
- Os dados analíticos oficiais e os eventos simulados são domínios separados. Evento de teste não é indicador oficial de alfabetização.
- `run_id` identifica uma execução. Consultas em tabelas Gold particionadas devem filtrar o `run_id` desejado.
- O status `PASSED` representa aprovação das regras implementadas; não prova completude integral da fonte original.
- Os arquivos completos de microdados não são versionados neste repositório público. Consulte o guia em `data/exports_raw/LEIA-ME.md`.

## Data de referência

Resultados AWS e FinOps consolidados com base nas evidências disponíveis em **29 de outubro de 2026**. Uma nova execução deve gerar novo registro, sem sobrescrever os resultados históricos.
