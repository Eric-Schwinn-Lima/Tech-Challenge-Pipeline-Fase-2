from pathlib import Path
import pandas as pd

run_id = "20261031T142554Z_9f2ed666"

gold = Path("data/gold") / f"run_id={run_id}"
silver = Path("data/silver") / f"run_id={run_id}"

traj = pd.read_parquet(
    gold / "mart_trajetoria_metas.parquet"
)

fact = pd.read_parquet(
    gold / "fato_alfabetizacao_municipio.parquet"
)

targets_muni = pd.read_parquet(
    silver / "meta_alfabetizacao_municipio.parquet"
)

targets_uf = pd.read_parquet(
    silver / "meta_alfabetizacao_uf.parquet"
)

print("\n" + "=" * 75)
print("1. METAS NULAS POR GEOGRAFIA E ANO")
print("=" * 75)

nulas = traj[traj["meta_alfabetizacao"].isna()]

print(nulas.groupby(
    ["nivel_geografico", "ano_meta", "meta_snapshot_ano"],
    dropna=False
).size())

print("\nExemplos de metas nulas:")
print(nulas.head(15).to_string(index=False))

print("\n" + "=" * 75)
print("2. VARIACOES AUSENTES DEPOIS DE 2024")
print("=" * 75)

variacoes_ausentes = traj[
    (traj["ano_meta"] != 2024)
    & traj["variacao_meta_pp_ano_anterior"].isna()
]

print(f"Total: {len(variacoes_ausentes)}")

print("\nDistribuicao por ano:")
print(variacoes_ausentes.groupby("ano_meta").size())

print("\nExemplos:")
print(variacoes_ausentes.head(15).to_string(index=False))

print("\n" + "=" * 75)
print("3. DISTRIBUICAO DOS STATUS POR REDE")
print("=" * 75)

print(pd.crosstab(
    fact["rede"],
    fact["status_meta_municipio"],
    margins=True
))

print("\nMotivos para ausencia de meta municipal:")
print(
    fact["motivo_sem_meta_municipio"]
    .value_counts(dropna=False)
)

print("\nRedes existentes nas tabelas de metas:")

print("Metas municipais:")
print(sorted(targets_muni["rede"].dropna().astype(str).unique()))

print("Metas estaduais:")
print(sorted(targets_uf["rede"].dropna().astype(str).unique()))

print("\n" + "=" * 75)
print("4. RESULTADOS OFICIAIS AUSENTES")
print("=" * 75)

print("\nTaxa municipal nula por rede:")
print(
    fact.assign(
        taxa_nula=fact["taxa_alfabetizacao"].isna()
    ).groupby("rede")["taxa_nula"].sum()
)

print("\nStatus municipal por ano:")
print(pd.crosstab(
    fact["ano"],
    fact["status_meta_municipio"]
))
