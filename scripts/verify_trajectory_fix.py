from pathlib import Path
import pandas as pd

run_id = "20261031T143349Z_7dfff85c"

gold = Path("data/gold") / f"run_id={run_id}"
silver = Path("data/silver") / f"run_id={run_id}"

traj = pd.read_parquet(
    gold / "mart_trajetoria_metas.parquet"
)

municipios = pd.read_parquet(
    silver / "municipio.parquet"
)

municipais = traj[
    traj["nivel_geografico"] == "MUNICIPIO"
].copy()

print("\n" + "=" * 70)
print("1. PREENCHIMENTO DA UF NA TRAJETORIA MUNICIPAL")
print("=" * 70)

print(f"Total de metas municipais: {len(municipais)}")
print(f"UF preenchida: {municipais['sigla_uf'].notna().sum()}")
print(f"UF nula: {municipais['sigla_uf'].isna().sum()}")

print("\nRegistros por UF:")
print(municipais.groupby("sigla_uf").size())

print("\nExemplos:")
print(
    municipais[
        ["id_municipio", "sigla_uf", "ano_meta", "meta_alfabetizacao"]
    ].head(10).to_string(index=False)
)

print("\n" + "=" * 70)
print("2. CONSISTENCIA DA DIMENSAO MUNICIPAL")
print("=" * 70)

dim = municipios[["id_municipio", "sigla_uf"]].dropna().copy()

conflitos = (
    dim.groupby("id_municipio")["sigla_uf"]
    .nunique()
)

print(f"Municípios na dimensão: {dim['id_municipio'].nunique()}")
print(f"Municípios com mais de uma UF: {(conflitos > 1).sum()}")

ids_dimensao = set(dim["id_municipio"].astype(str))
ids_trajetoria = set(municipais["id_municipio"].dropna().astype(str))

print(
    "IDs municipais da trajetória sem correspondência na dimensão:",
    len(ids_trajetoria - ids_dimensao)
)

print("\n" + "=" * 70)
print("3. SNAPSHOTS E METAS NULAS")
print("=" * 70)

print("\nSnapshots:")
print(traj.groupby("meta_snapshot_ano", dropna=False).size())

print("\nMetas nulas por geografia:")
print(
    traj[traj["meta_alfabetizacao"].isna()]
    .groupby(["nivel_geografico", "ano_meta"], dropna=False)
    .size()
)

