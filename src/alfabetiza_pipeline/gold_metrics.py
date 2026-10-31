from __future__ import annotations

import pandas as pd


LEVEL_COLUMNS = [f"proporcao_aluno_nivel_{level}" for level in range(9)]
PROFILE_COLUMNS = [
    "nivel_geografico", "ano", "sigla_uf", "id_municipio", "serie", "rede",
    "nivel_desempenho", "proporcao_alunos_pct", "soma_niveis_pct",
    "media_portugues", "taxa_alfabetizacao", "origem_dado",
]


def build_proficiency_profile(municipality: pd.DataFrame, uf: pd.DataFrame) -> pd.DataFrame:
    """Convert the source's nine proficiency-level shares into a query-friendly long mart."""
    outputs = []
    for geography, source in [("MUNICIPIO", municipality), ("UF", uf)]:
        if source.empty:
            continue
        level_columns = [column for column in LEVEL_COLUMNS if column in source.columns]
        if not level_columns:
            continue
        base_columns = [
            column for column in
            ["ano", "sigla_uf", "id_municipio", "serie", "rede", "media_portugues", "taxa_alfabetizacao"]
            if column in source.columns
        ]
        base = source[base_columns].copy()
        if "id_municipio" not in base.columns:
            base["id_municipio"] = pd.NA
        if "sigla_uf" not in base.columns:
            base["sigla_uf"] = pd.NA
        base["nivel_geografico"] = geography
        base["soma_niveis_pct"] = source[level_columns].sum(axis=1, min_count=1).to_numpy()
        if "media_portugues" not in base.columns:
            base["media_portugues"] = pd.NA
        if "taxa_alfabetizacao" not in base.columns:
            base["taxa_alfabetizacao"] = pd.NA
        long = base.melt(
            id_vars=[
                "nivel_geografico", "ano", "sigla_uf", "id_municipio",
                "serie", "rede", "soma_niveis_pct", "media_portugues",
                "taxa_alfabetizacao",
            ],
            value_vars=level_columns,
            var_name="campo_nivel",
            value_name="proporcao_alunos_pct",
        )
        long["nivel_desempenho"] = long["campo_nivel"].str.rsplit("_", n=1).str[-1].astype("int64")
        long = long.drop(columns=["campo_nivel"])
        long = long[long["proporcao_alunos_pct"].notna()].copy()
        long["origem_dado"] = "oficial_tabela_agregada"
        outputs.append(long[PROFILE_COLUMNS])
    if not outputs:
        return pd.DataFrame(columns=PROFILE_COLUMNS)
    return pd.concat(outputs, ignore_index=True)


def _monitoring_rows(frame: pd.DataFrame, geography: str) -> pd.DataFrame:
    columns = [
        "nivel_geografico", "sigla_uf", "ano", "serie", "rede",
        "total_registros", "registros_com_resultado", "universo_comparavel",
        "atingida", "abaixo_da_meta", "sem_meta", "sem_resultado", "nao_comparavel",
        "registros_com_meta_e_resultado", "percentual_cobertura_meta",
        "percentual_atingimento_entre_comparaveis", "mediana_gap_meta_pp",
        "pior_gap_meta_pp", "origem_dado",
    ]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    data = frame.copy()
    data["nivel_geografico"] = geography
    if "sigla_uf" not in data.columns:
        data["sigla_uf"] = pd.NA
    status_column = "status_meta_municipio" if geography == "MUNICIPIOS_POR_UF" else "status_meta_uf"
    gap_column = "gap_meta_municipio_pp" if geography == "MUNICIPIOS_POR_UF" else "gap_meta_uf_pp"
    group_columns = ["nivel_geografico", "sigla_uf", "ano", "serie", "rede"]
    rows = []
    for keys, group in data.groupby(group_columns, dropna=False, sort=True):
        counts = group[status_column].value_counts(dropna=False)
        attained = int(counts.get("ATINGIDA", 0))
        below = int(counts.get("ABAIXO_DA_META", 0))
        no_target = int(counts.get("SEM_META", 0))
        no_result = int(counts.get("SEM_RESULTADO", 0))
        not_comparable = int(counts.get("NAO_COMPARAVEL", 0))
        comparable = attained + below + no_target + no_result
        with_result_and_target = attained + below
        gaps = pd.to_numeric(group[gap_column], errors="coerce").dropna()
        row = dict(zip(group_columns, keys))
        row.update({
            "total_registros": int(len(group)),
            "registros_com_resultado": int(group["taxa_alfabetizacao"].notna().sum()),
            "universo_comparavel": comparable,
            "atingida": attained,
            "abaixo_da_meta": below,
            "sem_meta": no_target,
            "sem_resultado": no_result,
            "nao_comparavel": not_comparable,
            "registros_com_meta_e_resultado": with_result_and_target,
            "percentual_cobertura_meta": (with_result_and_target / comparable * 100) if comparable else pd.NA,
            "percentual_atingimento_entre_comparaveis": (attained / with_result_and_target * 100) if with_result_and_target else pd.NA,
            "mediana_gap_meta_pp": float(gaps.median()) if not gaps.empty else pd.NA,
            "pior_gap_meta_pp": float(gaps.min()) if not gaps.empty else pd.NA,
            "origem_dado": "derivado_status_meta_oficial",
        })
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)


def build_goal_monitoring(fact: pd.DataFrame, uf_mart: pd.DataFrame) -> pd.DataFrame:
    """Summarize target coverage and attainment; denominators are explicit in each row."""
    municipal = _monitoring_rows(fact, "MUNICIPIOS_POR_UF")
    state = _monitoring_rows(uf_mart, "UF_OFICIAL")
    result = pd.concat([municipal, state], ignore_index=True)
    return result


def build_goal_trajectory(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Unify target curves without mixing future targets with observed outcomes."""
    definitions = [
        ("BRASIL", "meta_alfabetizacao_brasil"),
        ("UF", "meta_alfabetizacao_uf"),
        ("MUNICIPIO", "meta_alfabetizacao_municipio"),
    ]
    outputs = []
    for geography, table_name in definitions:
        source = silver[table_name]
        if source.empty:
            continue
        target = pd.DataFrame({
            "nivel_geografico": geography,
            "sigla_uf": source["sigla_uf"] if "sigla_uf" in source.columns else pd.NA,
            "id_municipio": source["id_municipio"] if "id_municipio" in source.columns else pd.NA,
            "ano_meta": source["ano"],
            "serie": source["serie"],
            "rede": source["rede"],
            "meta_alfabetizacao": source["meta_alfabetizacao"],
            "meta_snapshot_ano": source["meta_snapshot_ano"] if "meta_snapshot_ano" in source.columns else pd.NA,
        })
        outputs.append(target)
    columns = [
        "nivel_geografico", "sigla_uf", "id_municipio", "ano_meta", "serie",
        "rede", "meta_alfabetizacao", "meta_snapshot_ano",
        "variacao_meta_pp_ano_anterior", "origem_dado",
    ]
    if not outputs:
        return pd.DataFrame(columns=columns)
    result = pd.concat(outputs, ignore_index=True)
    group_columns = ["nivel_geografico", "sigla_uf", "id_municipio", "serie", "rede"]
    result = result.sort_values(group_columns + ["ano_meta"], kind="stable").reset_index(drop=True)
    previous_year = result.groupby(group_columns, dropna=False)["ano_meta"].shift(1)
    previous_target = result.groupby(group_columns, dropna=False)["meta_alfabetizacao"].shift(1)
    variation = pd.to_numeric(result["meta_alfabetizacao"], errors="coerce") - pd.to_numeric(previous_target, errors="coerce")
    result["variacao_meta_pp_ano_anterior"] = variation.where(
        pd.to_numeric(result["ano_meta"], errors="coerce") - pd.to_numeric(previous_year, errors="coerce") == 1
    )
    result["origem_dado"] = "meta_oficial_snapshot"
    return result[columns]


def build_temporal_evolution(municipality: pd.DataFrame) -> pd.DataFrame:
    """Keep the existing simple mean and add year-over-year deltas for matched municipalities."""
    base = municipality.groupby(["ano", "serie", "rede"], dropna=False).agg(
        media_simples_municipal_percentual=("taxa_alfabetizacao", "mean"),
        municipios_com_resultado=("id_municipio", "nunique"),
    ).reset_index()
    paired = municipality[["ano", "id_municipio", "serie", "rede", "taxa_alfabetizacao"]].copy()
    paired = paired.sort_values(["id_municipio", "serie", "rede", "ano"], kind="stable")
    grouping = ["id_municipio", "serie", "rede"]
    paired["ano_anterior"] = paired.groupby(grouping, dropna=False)["ano"].shift(1)
    paired["taxa_ano_anterior"] = paired.groupby(grouping, dropna=False)["taxa_alfabetizacao"].shift(1)
    paired["variacao_municipal_pp"] = paired["taxa_alfabetizacao"] - paired["taxa_ano_anterior"]
    paired = paired[
        paired["ano_anterior"].notna()
        & ((paired["ano"] - paired["ano_anterior"]) == 1)
        & paired["taxa_alfabetizacao"].notna()
        & paired["taxa_ano_anterior"].notna()
    ].copy()
    if paired.empty:
        comparison = pd.DataFrame(columns=[
            "ano", "serie", "rede", "municipios_comparaveis_ano_anterior",
            "media_variacao_municipal_pp", "municipios_melhoraram",
            "municipios_pioraram", "municipios_estaveis",
        ])
    else:
        paired["melhorou"] = paired["variacao_municipal_pp"] > 0
        paired["piorou"] = paired["variacao_municipal_pp"] < 0
        paired["estavel"] = paired["variacao_municipal_pp"] == 0
        comparison = paired.groupby(["ano", "serie", "rede"], dropna=False).agg(
            municipios_comparaveis_ano_anterior=("id_municipio", "nunique"),
            media_variacao_municipal_pp=("variacao_municipal_pp", "mean"),
            municipios_melhoraram=("melhorou", "sum"),
            municipios_pioraram=("piorou", "sum"),
            municipios_estaveis=("estavel", "sum"),
        ).reset_index()
    result = base.merge(comparison, on=["ano", "serie", "rede"], how="left")
    count_columns = [
        "municipios_comparaveis_ano_anterior", "municipios_melhoraram",
        "municipios_pioraram", "municipios_estaveis",
    ]
    for column in count_columns:
        result[column] = result[column].fillna(0).astype("int64")
    result["indicador_agregado"] = "media_simples_municipal_nao_oficial"
    result["indicador_variacao"] = "variacao_ano_a_ano_municipios_pareados_nao_oficial"
    result["origem_dado"] = "oficial_tabela_agregada_agregada_sem_ponderacao"
    return result
