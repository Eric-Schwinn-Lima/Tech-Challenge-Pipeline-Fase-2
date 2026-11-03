from pathlib import Path
import pyarrow.parquet as pq

run_id = "20261031T142554Z_9f2ed666"
gold = Path("data/gold") / f"run_id={run_id}"

arquivos = [
    "mart_perfil_proficiencia.parquet",
    "mart_acompanhamento_metas.parquet",
    "mart_trajetoria_metas.parquet",
]

for nome in arquivos:
    caminho = gold / nome

    print("\n" + "=" * 90)
    print(nome)

    parquet = pq.ParquetFile(caminho)
    print(f"Total de linhas: {parquet.metadata.num_rows}")
    print("Colunas:")
    print(parquet.schema_arrow.names)

    tabela = pq.read_table(caminho)
    print("\nPrimeiros 5 registros:")
    print(tabela.slice(0, 5).to_pandas().to_string(index=False))
