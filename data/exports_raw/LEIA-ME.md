# Dados de origem fornecidos

O arquivo `dados_oficiais_base_dados.zip` contém seis tabelas do dataset `br_inep_avaliacao_alfabetizacao`: `alunos`, `municipio`, `uf` e as três tabelas de metas. A tabela auxiliar oficial `dicionario.csv` foi fornecida separadamente e está preservada nesta pasta.

`prepare_real_data.py` prepara os seis arquivos de análise e copia `dicionario.csv` para `data/source/dicionario.csv`. O dicionário é usado para decodificar `rede`, `serie`, `presenca` e `alfabetizado` onde os códigos são compatíveis, e para derivar as chaves numéricas correspondentes às categorias textuais nas tabelas de metas.

**Possível limite do microdado:** `alunos.csv` contém exatamente 1.048.575 registros de dados. Isso é compatível com o limite de 1.048.576 linhas de uma planilha Excel contando o cabeçalho. É um forte indício de possível truncamento, mas não prova que os dados foram truncados. Confirme a contagem completa por BigQuery ou pelo pacote de acesso da Base dos Dados antes de declarar a reconciliação dos microdados como conclusiva.

Os eventos em `data/events/stream_events.jsonl` são artificiais e servem somente para demonstrar streaming. Eles não fazem parte dos dados oficiais.
