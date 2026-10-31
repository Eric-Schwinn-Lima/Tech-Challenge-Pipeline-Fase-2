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


## 9. Produtos analíticos Gold adicionais

A implementação mais recente do código acrescenta os seguintes produtos. As quantidades publicadas na seção de evidências AWS continuam descrevendo uma execução anterior, que materializou quatro tabelas Gold; as novas tabelas só devem ser contabilizadas como produtos executados após uma nova execução do Glue e atualização do Data Catalog.

| Tabela | Grão | Objetivo |
|---|---|---|
| \`mart_perfil_proficiencia\` | geografia + ano + UF/município + série + rede + nível 0–8 | Distribuição percentual dos alunos pelos nove níveis, preservando \`media_portugues\` e \`taxa_alfabetizacao\` como medidas de contexto. |
| \`mart_acompanhamento_metas\` | geografia + UF + ano + série + rede | Contagens por status, cobertura de meta, atingimento entre registros com meta e resultado, mediana e pior gap em pontos percentuais. |
| \`mart_trajetoria_metas\` | geografia + entidade + ano-alvo + série + rede | Curva de metas de Brasil, UF e município de 2024 a 2030, preservando o ano do snapshot e a variação anual da meta. |

### 9.1. Perfil de proficiência

\`mart_perfil_proficiencia\` transforma as colunas \`proporcao_aluno_nivel_0\` a \`proporcao_aluno_nivel_8\` para o formato longo:

- \`nivel_geografico\`: \`MUNICIPIO\` ou \`UF\`;
- \`nivel_desempenho\`: inteiro de 0 a 8;
- \`proporcao_alunos_pct\`: percentual da fonte para o nível;
- \`soma_niveis_pct\`: soma das proporções disponíveis na linha;
- \`media_portugues\`: média de proficiência em Língua Portuguesa da fonte;
- \`taxa_alfabetizacao\`: indicador publicado que permanece independente.

A mart não recalcula nem substitui a taxa oficial de alfabetização a partir dos níveis. Não se deve somar níveis inteiros para inferir o corte individual de alfabetização: o limite pode atravessar uma faixa de proficiência. As proporções são preservadas por nível para permitir análises e validações sem atribuir significado não documentado a agrupamentos de níveis.

### 9.2. Acompanhamento de metas

A tabela agrega os estados \`ATINGIDA\`, \`ABAIXO_DA_META\`, \`SEM_META\`, \`SEM_RESULTADO\` e \`NAO_COMPARAVEL\`.

Definições dos denominadores:

- \`universo_comparavel\`: registros cujo código de rede pertence ao universo coberto pela tabela de metas; inclui registros sem meta ou sem resultado.
- \`registros_com_meta_e_resultado\`: soma de \`ATINGIDA\` e \`ABAIXO_DA_META\`.
- \`percentual_cobertura_meta\`: \`registros_com_meta_e_resultado / universo_comparavel × 100\`.
- \`percentual_atingimento_entre_registros_com_meta_e_resultado\`: \`ATINGIDA / registros_com_meta_e_resultado × 100\`.

A \`mediana_gap_meta_pp\` e o \`pior_gap_meta_pp\` consideram somente gaps calculáveis, isto é, registros com resultado e meta. O pior gap é o menor valor (mais negativo). Os níveis geográficos são \`MUNICIPIOS_POR_UF\` e \`UF_OFICIAL\`; não se deve somar esses grupos entre si, pois representam universos diferentes.

### 9.3. Trajetória de metas

\`mart_trajetoria_metas\` consolida os três níveis de meta sem misturar projeções/anos-alvo futuros com resultados observados. \`ano_meta\` é o ano ao qual a meta se aplica; \`meta_snapshot_ano\` identifica o snapshot de origem quando fornecido pela preparação. \`variacao_meta_pp_ano_anterior\` só é preenchida quando há um ano-alvo imediatamente anterior para a mesma entidade, série e rede.

### 9.4. Evolução temporal e reconciliação

\`mart_evolucao_temporal\` mantém \`media_simples_municipal_percentual\` como média simples descritiva, não oficial e não ponderada. A nova comparação ano a ano usa apenas municípios com taxa publicada em dois anos consecutivos, na mesma série e rede, e registra a variação média em pontos percentuais e as contagens de municípios que melhoraram, pioraram ou permaneceram estáveis. Uma lacuna de ano não é tratada como comparação consecutiva.

\`reconciliacao_microdados\` passa a registrar \`tem_registro_publicado\` e \`status_correspondencia\` (\`COMPARAVEL\`, \`SEM_REGISTRO_PUBLICADO\` ou \`REGISTRO_SEM_TAXA_PUBLICADA\`). A diferença entre o indicador calculado com pesos amostrais e a taxa publicada só é interpretável quando há taxa oficial publicada. A possível incompletude do arquivo de microdados permanece uma limitação conhecida.

A fato municipal e a mart de UF também passam a expor \`gap_meta_brasil_pp\` e \`status_meta_brasil\`, para recortes de rede compatíveis com a tabela de meta nacional. Esses campos não substituem os gaps municipais e estaduais.

### 9.5. Qualidade adicional

O Quality Gate verifica os domínios binários de \`presenca\` e \`alfabetizado\` (0 ou 1) e o intervalo de 0 a 100 para cada proporção de nível disponibilizada. A soma das proporções dos níveis é registrada como diagnóstico informativo, não como bloqueio, até que arredondamento e cobertura da fonte sejam confirmados. Ausências continuam tratadas pelas regras específicas de cada variável.

## 10. Linhagem

A fonte original fica em `data/exports_raw/` (local, não versionada integralmente). A versão canônica fica em `data/source/`. No S3, o prefixo `bronze/raw_exports/` preserva cópias brutas e `bronze/source/` contém os CSVs canônicos esperados pelo Glue. Silver/Gold são produzidas por `run_id`.
