# Evidência AWS — Quality Gate

## Objetivo
Comprovar que uma duplicidade na chave natural de municipio
interrompe o processamento antes da publicação das camadas seguintes.

## Execução

- Serviço: AWS Glue
- Região: us-east-2
- Data: 30/10/2026
- JobRunId: jr_59f157b16876a5d794504c72f8b7e5ae34f361779a7639232c24082e3a6d846d
- Run ID: 20261030T032201Z_ed47f526

## Resultado

- Estado final do Glue: FAILED
- Regra: duplicate_natural_keys:municipio
- Resultado: FAIL
- Duplicidades detectadas: 1
- Objetos Silver de teste: 0
- Objetos Gold de teste: 0

## Relatórios

Quality:
s3://tc-alfabetizacao-a9455abd-lake/observability/quality/run_id=20261030T032201Z_ed47f526.json

Metrics:
s3://tc-alfabetizacao-a9455abd-lake/observability/metrics/run_id=20261030T032201Z_ed47f526.json

## Conclusão

O AWS Glue identificou a duplicidade, registrou a falha
e interrompeu o processamento antes da publicação Silver
e Gold. Os prefixos de produção não foram utilizados.
