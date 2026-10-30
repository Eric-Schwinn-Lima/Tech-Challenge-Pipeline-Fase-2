# Dicionário de dados e regras de canonicalização

## 1. Fontes

O projeto utiliza seis tabelas analíticas oficiais da Base dos Dados e o dicionário oficial auxiliar `dicionario.csv`.

| Arquivo canônico | Conteúdo | Grão/observação |
|---|---|---|
| `uf.csv` | Indicadores agregados por UF | ano + UF + série + rede |
| `municipio.csv` | Indicadores agregados por município | ano + município + série + rede |
| `meta_alfabetizacao_brasil.csv` | Metas nacionais | ano-alvo + série + rede |
| `meta_alfabetizacao_uf.csv` | Metas por UF | ano-alvo + UF + série + rede |
| `meta_alfabetizacao_municipio.csv` | Metas por município | ano-alvo + município + série + rede |
| `alunos.csv` | Microdados de alunos | aluno + ano + município + série + rede; validar a chave na fonte |
| `dicionario.csv` | Descrições e cobertura dos códigos categóricos | tabela + coluna + chave |

Os seis arquivos analíticos podem vir do ZIP oficial ou de CSVs separados. O dicionário é fornecido separadamente e deve ser mantido junto aos arquivos brutos.

## 2. Campos do dicionário oficial

O `dicionario.csv` exige estas colunas:

| Campo | Descrição |
|---|---|
| `id_tabela` | Identificador da tabela a que a chave pertence |
| `nome_coluna` | Nome da coluna categórica |
| `chave` | Código original armazenado na tabela |
| `cobertura_temporal` | Cobertura temporal indicada pela fonte |
| `valor` | Rótulo/descrição oficial da chave |

A preparação valida duplicidade pela combinação `id_tabela`, `nome_coluna` e `chave`. As descrições são adicionadas em colunas auxiliares; os códigos originais são preservados para junções.

## 3. Campos canônicos relevantes

### Tabelas agregadas

As tabelas `municipio` e `uf` preservam os campos da fonte e padronizam nomes em minúsculas. A tabela municipal recebe `sigla_uf`, derivada deterministicamente dos dois primeiros dígitos do código IBGE de sete dígitos. O código original do município permanece preservado.

Campos de identificação usados pelo pipeline:

- `ano`
- `id_municipio` (tabela municipal)
- `sigla_uf` (tabela estadual)
- `serie`
- `rede`
- `taxa_alfabetizacao` (indicador agregado usado no fato)

A presença exata de outros campos deve ser conferida no CSV canônico e no inventário `data/exports_raw/source_inventory.json`.

### Microdados de alunos

A preparação exige no bruto: `ano`, `id_municipio`, `id_aluno`, `serie`, `rede`, `presenca`, `alfabetizado` e `peso_aluno`.

Transformações documentadas:

- normaliza `id_municipio` para código textual de sete dígitos;
- deriva `sigla_uf` pelo prefixo IBGE;
- renomeia `peso_aluno` para `peso_amostral`;
- adiciona `origem_dado = oficial_microdados_fornecidos`;
- processa em blocos de até 150.000 registros para reduzir uso de memória.

Pesos ausentes ou não positivos não devem ser tratados como pesos válidos; os registros são excluídos do cálculo ponderado conforme regra de qualidade.

### Tabelas de metas

Os CSVs originais apresentam colunas anuais de meta (2024 a 2030). A preparação converte o formato largo para longo:

- `ano`: ano-alvo da meta;
- `meta_snapshot_ano`: ano do snapshot original selecionado;
- `meta_alfabetizacao`: valor da meta;
- `serie`: preenchida com `2`, correspondente ao indicador de conclusão do 2º ano do ensino fundamental;
- `rede`: código numérico obtido do dicionário.

A seleção do snapshot usa o registro mais recente por entidade, conforme o ano disponível na fonte.

## 4. Códigos de rede

O dicionário possui códigos diferentes conforme a tabela. Não misture a codificação de microdados com a codificação das tabelas agregadas.

### Microdados `alunos`

| Código | Descrição |
|---:|---|
| 1 | Federal |
| 2 | Estadual |
| 3 | Municipal |
| 4 | Privada |

### Tabelas agregadas `municipio` e `uf`

| Código | Descrição |
|---:|---|
| 0 | Total |
| 1 | Federal |
| 2 | Estadual |
| 3 | Municipal |
| 4 | Privada |
| 5 | Pública (Estadual e Municipal) |
| 6 | Pública (Federal, Estadual e Municipal) |

### Mapeamento das metas

A preparação obtém os códigos usando o dicionário oficial:

- Meta municipal: rótulo `Municipal` → código `3`.
- Meta de UF e Brasil: rótulo curto `Pública` → código `5`, cuja descrição oficial é `Pública (Estadual e Municipal)`.

A fonte de metas usa o rótulo curto `Pública`; o dicionário não contém uma entrada literal idêntica a esse rótulo. O mapeamento para código 5 é uma equivalência semântica explícita do adaptador, registrada no inventário. Não interprete esse mapeamento como prova de equivalência universal fora do contexto desta fonte.

## 5. Chaves e joins

| Join | Chaves de referência |
|---|---|
| `alunos` → `municipio` | ano + id_municipio + série + rede, respeitando o grão/recorte compatível |
| `municipio` → `uf` | ano + sigla_uf + série + rede |
| `municipio` → meta municipal | ano-alvo + id_municipio + série + rede |
| `uf` → meta estadual | ano-alvo + sigla_uf + série + rede |
| indicador Brasil → meta Brasil | ano-alvo + série + rede |

Não elimine `ano`, `serie` ou `rede` para aumentar artificialmente a taxa de correspondência. As chaves efetivas devem respeitar a granularidade de cada tabela.

## 6. Estados de comparação com meta

A Gold distingue pelo menos os seguintes estados:

- `Atingida`: valor observado alcança a meta comparável.
- `abaixo`: valor observado fica abaixo da meta comparável.
- `SEM_META`: recorte que possui resultado, mas não possui meta aplicável preenchida/disponível.
- `NAO_COMPARAVEL`: rede ou recorte não corresponde ao universo coberto pela meta.

Ausência de meta não equivale a meta zero. `SEM_META` não deve ser interpretado como descumprimento.

## 7. Cobertura e limitações conhecidas

- As metas canônicas foram transformadas para anos-alvo de 2024 a 2030. Não há meta municipal para 2023 na extração validada.
- Para 2024, série 2 e rede municipal, o fato possuía 5.448 municípios/registros. Para 96 municípios não havia linha correspondente na fonte de metas; outros 120 tinham linha de meta, mas o valor estava vazio também no CSV bruto. Restaram 5.232 valores preenchidos nesse recorte.
- A validação da ausência dos 120 valores foi feita contra o arquivo bruto. A ausência de linha para os 96 não deve ser generalizada como erro do ETL sem nova análise individual.
- A fonte `alunos.csv` contém 1.048.575 registros, padrão compatível com possível limite de exportação de planilha. É alerta, não prova de truncamento.
- O pipeline não deve preencher metas ou taxas ausentes com zero.

## 8. Linhagem

A fonte original fica em `data/exports_raw/` (local, não versionada integralmente). A versão canônica fica em `data/source/`. No S3, o prefixo `bronze/raw_exports/` preserva cópias brutas e `bronze/source/` contém os CSVs canônicos esperados pelo Glue. Silver/Gold são produzidas por `run_id`.
