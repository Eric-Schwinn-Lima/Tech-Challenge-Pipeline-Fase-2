from pathlib import Path
import pandas as pd

run_id = "20261031T142554Z_9f2ed666"
gold = Path("data/gold") / f"run_id={run_id}"

# 1. Perfil de proficiência
perfil = pd.read_parquet(gold / "mart_perfil_proficiencia.parquet")

print("\n" + "=" * 75)
print("1. PERFIL DE PROFICIENCIA")
print("=" * 75)

print("\nRegistros por nivel geografico:")
print(perfil.groupby("nivel_geografico").size())

print("\nNiveis de desempenho encontrados:")
print(sorted(perfil["nivel_desempenho"].dropna().unique().tolist()))

chaves = [
    "nivel_geografico", "ano", "sigla_uf",
    "id_municipio", "serie", "rede", "origem_dado"
]

perfil_resumo = (
    perfil.groupby(chaves, dropna=False)
    .agg(
        niveis_encontrados=("nivel_desempenho", "nunique"),
        soma_calculada=("proporcao_alunos_pct", "sum"),
        soma_informada=("soma_niveis_pct", "first")
    )
    .reset_index()
)

print("\nQuantidade de grupos por numero de niveis:")
print(perfil_resumo["niveis_encontrados"].value_counts().sort_index())

print("\nResumo das somas das proporcoes:")
print(perfil_resumo[["soma_calculada", "soma_informada"]].describe())

print("\nGrupos com soma diferente de 100 (tolerancia 0.1):")
print(
    (~perfil_resumo["soma_calculada"].sub(100).abs().le(0.1)).sum()
)

# 2. Acompanhamento das metas
metas = pd.read_parquet(gold / "mart_acompanhamento_metas.parquet")

print("\n" + "=" * 75)
print("2. ACOMPANHAMENTO DAS METAS")
print("=" * 75)

status = [
    "atingida", "abaixo_da_meta",
    "sem_meta", "sem_resultado", "nao_comparavel"
]

metas["soma_status"] = metas[status].fillna(0).sum(axis=1)
metas["diferenca_status"] = (
    metas["total_registros"] - metas["soma_status"]
)

print("\nDiferencas entre total_registros e soma dos status:")
print(metas["diferenca_status"].value_counts(dropna=False))

print("\nTotais por classificacao:")
print(metas[status].sum())

print("\nPercentuais de cobertura e atingimento:")
print(
    metas[
        [
            "percentual_cobertura_meta",
            "percentual_atingimento_entre_registros_com_meta_e_resultado"
        ]
    ].describe()
)

print("\nRegistros com denominador valido para atingimento:")
print(metas["registros_com_meta_e_resultado"].gt(0).sum())

# 3. Trajetoria das metas
trajetoria = pd.read_parquet(gold / "mart_trajetoria_metas.parquet")

print("\n" + "=" * 75)
print("3. TRAJETORIA DAS METAS")
print("=" * 75)

print("\nRegistros por nivel geografico:")
print(trajetoria.groupby("nivel_geografico").size())

print("\nMetas por ano:")
print(trajetoria.groupby("ano_meta").size())

print("\nSnapshots utilizados:")
print(trajetoria.groupby("meta_snapshot_ano").size())

print("\nFaixa de valores das metas:")
print(pd.to_numeric(
    trajetoria["meta_alfabetizacao"], errors="coerce"
).describe())

print("\nAnos com variacao de meta calculada:")
print(trajetoria["variacao_meta_pp_ano_anterior"].notna().sum())

