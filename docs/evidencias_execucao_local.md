# Evidência da execução local com dados oficiais fornecidos

> Esta seção representa uma execução **local** em Windows/Python. Não é uma medição AWS. Os eventos são simulados. A completude dos microdados ainda precisa de confirmação na fonte original.

## 1. Identificação

- `run_id`: `20261029T003707Z_763b68ad`
- Estado: `SUCCESS`
- Início UTC: `2026-10-29T00:37:07.441Z`
- Término UTC: `2026-10-29T00:37:38.252Z`
- Duração medida: `30,811 s`
- Diretórios: `data/bronze/run_id=20261029T003707Z_763b68ad`, `data/silver/run_id=20261029T003707Z_763b68ad` e `data/gold/run_id=20261029T003707Z_763b68ad`.

## 2. Linhas processadas

| Fonte canônica | Registros |
|---|---:|
| `uf` | 145 |
| `municipio` | 23.995 |
| `meta_alfabetizacao_brasil` | 7 |
| `meta_alfabetizacao_uf` | 189 |
| `meta_alfabetizacao_municipio` | 37.464 |
| `alunos` | 1.048.575 |
| **Subtotal das seis entidades obrigatórias** | **1.110.375** |
| `dicionario` auxiliar | 27 |
| **Total incluindo dicionário** | **1.110.402** |

As metas foram transformadas de formato largo para linhas por ano-alvo (2024 a 2030). Por isso, a contagem canônica das metas pode ser maior que a contagem física dos registros no CSV original.

## 3. Qualidade, reconciliação e eventos

| Métrica | Resultado local |
|---|---:|
| Verificações de qualidade | 61 |
| Falhas bloqueantes | 0 |
| Correspondência `alunos → municipio` em redes comparáveis | 99,82% |
| Correspondência `municipio → uf` | 98,64% |
| Cobertura de municípios com meta municipal nos anos aplicáveis | 98,24% |
| Cobertura de municípios com meta estadual nos anos aplicáveis | 100% |
| Cobertura de municípios com meta nacional nos anos aplicáveis | 100% |
| Pesos amostrais ausentes/não positivos entre alunos presentes | 312 (0,0343%); excluídos do cálculo ponderado |
| Códigos numéricos sem rótulo nas colunas verificadas | 0 |
| Eventos simulados recebidos | 4 |
| Eventos válidos encaminhados à Gold | 2 |
| Eventos em quarentena | 1 |
| Eventos duplicados removidos | 1 |
| Bytes copiados para Bronze (raw + canônicos + eventos) | 162.132.540 |

## 4. Saídas Gold locais

| Tabela | Linhas |
|---|---:|
| `fato_alfabetizacao_municipio` | 23.995 |
| `mart_alfabetizacao_uf` | 145 |
| `mart_evolucao_temporal` | 7 |
| `reconciliacao_microdados` | 12.393 |
| `mart_streaming_eventos` | 2 |
| **Total** | **36.542** |

A Silver contém `dicionario` (27 linhas) e colunas descritivas como `rede_descricao`, `serie_descricao`, `presenca_descricao` e `alfabetizado_descricao` onde há correspondência oficial. Os códigos originais permanecem preservados.

## 5. Formato e recursos locais

Na execução registrada, PyArrow não estava instalado e a escrita local usou o fallback `.csv.gz`. Isso não deve ser descrito como Parquet. O job AWS Glue grava Parquet Snappy. O `pyproject.toml` declara PyArrow para o ambiente local padrão.

## 6. Limitações e interpretação

1. `alunos.csv` contém exatamente **1.048.575** registros, padrão compatível com o limite de 1.048.576 linhas de uma planilha Excel incluindo cabeçalho. É indício de possível truncamento, não prova definitiva. Confirme a quantidade completa na Base dos Dados/origem antes de declarar a reconciliação conclusiva.
2. A fonte das metas usa o rótulo curto `Pública`; o dicionário possui `Pública (Estadual e Municipal)` para a chave 5 nas tabelas agregadas. O adaptador documenta a equivalência usada; não generalize além desta fonte.
3. A execução local processou eventos simulados e gerou duas linhas em `mart_streaming_eventos`. Isso não significa que a execução AWS tenha produzido essa tabela: os eventos AWS testados não possuíam o esquema analítico completo e foram encaminhados para quarentena.
4. Os percentuais de cobertura são referentes aos recortes e regras dessa execução, não uma declaração universal de completude das fontes.
