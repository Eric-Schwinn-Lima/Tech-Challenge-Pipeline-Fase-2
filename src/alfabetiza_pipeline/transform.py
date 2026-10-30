from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ALIASES = {
    "ano": ["ano", "year", "ano_avaliacao", "ano_aplicacao"],
    "sigla_uf": ["sigla_uf", "uf", "estado"],
    "id_municipio": ["id_municipio", "codigo_municipio", "municipio_id", "id_municipio_ibge"],
    "serie": ["serie", "ano_escolar", "etapa"],
    "rede": ["rede", "rede_ensino", "tipo_rede"],
    "taxa_alfabetizacao": ["taxa_alfabetizacao", "proporcao_alfabetizados", "percentual_alfabetizados", "taxa"],
    "meta_alfabetizacao": ["meta_alfabetizacao", "meta", "meta_percentual"],
    "id_aluno": ["id_aluno", "codigo_aluno", "aluno_id"],
    "presenca": ["presenca", "presente", "indicador_presenca"],
    "alfabetizado": ["alfabetizado", "indicador_alfabetizado", "aluno_alfabetizado"],
    "peso_amostral": ["peso_amostral", "peso", "peso_aluno", "peso_final"],
    "proficiencia": ["proficiencia", "proficiência", "nota", "proficiência_saeb"],
}


def _normalize_name(name: str) -> str:
    import unicodedata
    value = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def canonicalize_columns(df: pd.DataFrame, table: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_name(c) for c in df.columns]
    rename = {}
    for canonical, possible_names in ALIASES.items():
        for alias in [_normalize_name(canonical), *[_normalize_name(a) for a in possible_names]]:
            if alias in df.columns:
                rename[alias] = canonical
                break
    df = df.rename(columns=rename)
    # Keep unknown columns. The dictionary of the source must be used to decode unknown category codes;
    # this code intentionally does not invent mappings for undocumented values.
    return df


def _to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype("string").str.replace(",", ".", regex=False), errors="coerce")


def _normalize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.lower().str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("ascii")


def _normalize_network(series: pd.Series) -> pd.Series:
    normalized = _normalize_text(series)
    return normalized.replace({
        "publica": "publica",
        "rede publica": "publica",
        "publica total": "publica",
        "rede_publica": "publica",
        "privada": "privada",
        "rede privada": "privada",
    })


def _normalize_series(series: pd.Series) -> pd.Series:
    # Remove ordinal markers before ASCII normalization so "2º ano" becomes "2", not "2o".
    raw = series.astype("string").str.strip().str.lower().str.replace("º", "", regex=False).str.replace("°", "", regex=False)
    values = raw.str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("ascii")
    values = values.str.replace("ano", "", regex=False).str.strip()
    return values


def normalize_table(table: str, df: pd.DataFrame) -> pd.DataFrame:
    df = canonicalize_columns(df, table)
    if table == "dicionario":
        required = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Tabela dicionario: faltam colunas obrigatórias: {missing}")
        for col in ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]:
            df[col] = df[col].astype("string").str.strip()
        df["id_tabela"] = df["id_tabela"].str.lower()
        df["nome_coluna"] = df["nome_coluna"].str.lower()
        return df
    required_by_table = {
        "uf": ["ano", "sigla_uf", "serie", "rede", "taxa_alfabetizacao"],
        "municipio": ["ano", "id_municipio", "sigla_uf", "serie", "rede", "taxa_alfabetizacao"],
        "meta_alfabetizacao_brasil": ["ano", "serie", "rede", "meta_alfabetizacao"],
        "meta_alfabetizacao_uf": ["ano", "sigla_uf", "serie", "rede", "meta_alfabetizacao"],
        "meta_alfabetizacao_municipio": ["ano", "id_municipio", "serie", "rede", "meta_alfabetizacao"],
        "alunos": ["id_aluno", "ano", "id_municipio", "sigla_uf", "serie", "rede", "presenca", "alfabetizado", "peso_amostral"],
    }
    missing = [col for col in required_by_table[table] if col not in df.columns]
    if missing:
        raise ValueError(f"Tabela {table}: faltam colunas obrigatórias após canonicalização: {missing}. Colunas recebidas: {list(df.columns)}")

    for col in ("ano",):
        df[col] = _to_number(df[col]).astype("Int64")
    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].astype("string").str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(7)
    if "sigla_uf" in df.columns:
        df["sigla_uf"] = df["sigla_uf"].astype("string").str.strip().str.upper()
    if "serie" in df.columns:
        df["serie"] = _normalize_series(df["serie"])
    if "rede" in df.columns:
        df["rede"] = _normalize_network(df["rede"])
    for col in ("taxa_alfabetizacao", "meta_alfabetizacao", "peso_amostral", "proficiencia"):
        if col in df.columns:
            df[col] = _to_number(df[col])
    for col in ("presenca", "alfabetizado"):
        if col in df.columns:
            normalized = _normalize_text(df[col])
            mapping = {"1": 1, "0": 0, "true": 1, "false": 0, "sim": 1, "nao": 0, "s": 1, "n": 0, "presente": 1, "ausente": 0, "yes": 1, "no": 0, "alfabetizado": 1, "nao alfabetizado": 0}
            df[col] = normalized.map(mapping).fillna(_to_number(df[col])).astype("Float64")
    if "id_aluno" in df.columns:
        df["id_aluno"] = df["id_aluno"].astype("string").str.strip()
    return df


def load_events(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    if not path.exists():
        return pd.DataFrame(), pd.DataFrame(), 0
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as file:
        for line_no, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                records.append({"_line_no": line_no, "_parse_error": "invalid_json", "raw_line": line.strip()})
    events = pd.DataFrame(records)
    if events.empty:
        return events, pd.DataFrame(), 0
    duplicate_count = int(events["event_id"].duplicated(keep="first").sum()) if "event_id" in events.columns else 0
    if "event_id" in events.columns:
        events = events.drop_duplicates(subset=["event_id"], keep="first").copy()

    required = ["event_id", "event_timestamp", "ano", "id_municipio", "sigla_uf", "serie", "rede", "taxa_alfabetizacao"]
    invalid_reasons = []
    for _, row in events.iterrows():
        reasons = []
        for col in required:
            if col not in events.columns or pd.isna(row.get(col)) or str(row.get(col)).strip() == "":
                reasons.append(f"missing:{col}")
        try:
            rate = float(str(row.get("taxa_alfabetizacao")).replace(",", "."))
            if not 0 <= rate <= 100:
                reasons.append("taxa_alfabetizacao_out_of_range")
        except (TypeError, ValueError):
            reasons.append("taxa_alfabetizacao_not_numeric")
        try:
            produced = pd.Timestamp(str(row.get("event_timestamp")))
            if produced.tzinfo is None:
                produced = produced.tz_localize("UTC")
            if produced > pd.Timestamp.now(tz="UTC"):
                reasons.append("event_timestamp_in_future")
        except Exception:
            reasons.append("invalid_event_timestamp")
        invalid_reasons.append(";".join(reasons))
    events["validation_error"] = invalid_reasons
    quarantine = events[events["validation_error"] != ""].copy()
    valid = events[events["validation_error"] == ""].drop(columns=["validation_error"]).copy()
    if not valid.empty:
        valid["ano"] = _to_number(valid["ano"]).astype("Int64")
        valid["id_municipio"] = valid["id_municipio"].astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(7)
        valid["sigla_uf"] = valid["sigla_uf"].astype("string").str.upper().str.strip()
        valid["serie"] = _normalize_series(valid["serie"])
        valid["rede"] = _normalize_network(valid["rede"])
        valid["taxa_alfabetizacao"] = _to_number(valid["taxa_alfabetizacao"])
        valid["origem_dado"] = valid.get("origem_dado", pd.Series("simulado", index=valid.index)).fillna("simulado")
        valid["ingested_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        event_time = pd.to_datetime(valid["event_timestamp"], utc=True, errors="coerce")
        ingest_time = pd.to_datetime(valid["ingested_at"], utc=True, errors="coerce")
        valid["latencia_segundos"] = (ingest_time - event_time).dt.total_seconds()
        valid["origem_dado"] = valid["origem_dado"].replace("", "simulado")
    return valid, quarantine, duplicate_count


def _dictionary_key(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _apply_dictionary_labels(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Add official *_descricao columns without replacing source category codes."""
    dictionary = tables.get("dicionario")
    if dictionary is None or dictionary.empty:
        return tables
    entity_map = {
        "alunos": "alunos",
        "municipio": "municipio",
        "uf": "uf",
        "meta_alfabetizacao_municipio": "municipio",
        "meta_alfabetizacao_uf": "uf",
        # The national public target uses the same public-network coding as the UF metadata.
        "meta_alfabetizacao_brasil": "uf",
    }
    category_columns = ["rede", "serie", "presenca", "alfabetizado", "preenchimento_caderno"]
    out = dict(tables)
    for table_name, dictionary_entity in entity_map.items():
        frame = out.get(table_name)
        if frame is None or frame.empty:
            continue
        entity_dict = dictionary[dictionary["id_tabela"].astype("string").str.lower().eq(dictionary_entity)]
        if entity_dict.empty:
            continue
        frame = frame.copy()
        for column in category_columns:
            if column not in frame.columns:
                continue
            lookup = entity_dict[entity_dict["nome_coluna"].astype("string").str.lower().eq(column)][["chave", "valor"]].dropna(subset=["chave"]).copy()
            if lookup.empty:
                continue
            lookup["_dictionary_key"] = lookup["chave"].map(_dictionary_key)
            lookup = lookup.drop_duplicates("_dictionary_key", keep="last")
            value_map = dict(zip(lookup["_dictionary_key"], lookup["valor"].astype("string")))
            frame[f"{column}_descricao"] = frame[column].map(_dictionary_key).map(value_map).astype("string")
        out[table_name] = frame
    return out


def transform_to_silver(raw_tables: dict[str, pd.DataFrame], events_file: Path) -> dict[str, pd.DataFrame]:
    silver = {name: normalize_table(name, frame) for name, frame in raw_tables.items()}
    silver = _apply_dictionary_labels(silver)
    valid_events, quarantine, duplicate_count = load_events(events_file)
    silver["streaming_events"] = valid_events
    silver["streaming_quarantine"] = quarantine
    silver["streaming_duplicate_count"] = duplicate_count  # metadata consumed by quality gate

    students = silver["alunos"].copy()
    assessed = students[
        (students["presenca"] == 1)
        & students["alfabetizado"].notna()
        & students["peso_amostral"].notna()
        & (students["peso_amostral"] > 0)
    ].copy()
    if assessed.empty:
        silver["indicador_calculado_microdados"] = pd.DataFrame(columns=["ano", "id_municipio", "sigla_uf", "serie", "rede", "taxa_calculada_microdados", "peso_avaliado"])
    else:
        assessed["peso_alfabetizado"] = assessed["alfabetizado"].astype(float) * assessed["peso_amostral"].astype(float)
        grp = assessed.groupby(["ano", "id_municipio", "sigla_uf", "serie", "rede"], dropna=False).agg(
            peso_alfabetizado=("peso_alfabetizado", "sum"),
            peso_avaliado=("peso_amostral", "sum"),
            alunos_avaliados=("id_aluno", "count"),
        ).reset_index()
        grp["taxa_calculada_microdados"] = grp["peso_alfabetizado"] / grp["peso_avaliado"] * 100
        silver["indicador_calculado_microdados"] = grp.drop(columns=["peso_alfabetizado"])
    return silver


def build_gold(silver: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    municipality = silver["municipio"].copy()
    if municipality.empty:
        raise ValueError("Tabela municipio vazia: não é possível criar a Gold.")

    target_muni = silver["meta_alfabetizacao_municipio"].rename(columns={"meta_alfabetizacao": "meta_municipio"})
    fact = municipality.merge(target_muni[["ano", "id_municipio", "serie", "rede", "meta_municipio"]], on=["ano", "id_municipio", "serie", "rede"], how="left", validate="many_to_one")

    target_uf = silver["meta_alfabetizacao_uf"].rename(columns={"meta_alfabetizacao": "meta_uf"})
    fact = fact.merge(target_uf[["ano", "sigla_uf", "serie", "rede", "meta_uf"]], on=["ano", "sigla_uf", "serie", "rede"], how="left", validate="many_to_one")
    target_br = silver["meta_alfabetizacao_brasil"].rename(columns={"meta_alfabetizacao": "meta_brasil"})
    fact = fact.merge(target_br[["ano", "serie", "rede", "meta_brasil"]], on=["ano", "serie", "rede"], how="left", validate="many_to_one")
    fact["gap_meta_municipio_pp"] = fact["taxa_alfabetizacao"] - fact["meta_municipio"]
    comparable_muni_networks = set(target_muni["rede"].dropna().astype(str))
    comparable_muni = fact["rede"].astype(str).isin(comparable_muni_networks)
    fact["status_meta_municipio"] = "NAO_COMPARAVEL"
    fact.loc[comparable_muni, "status_meta_municipio"] = "SEM_META"
    fact.loc[comparable_muni & fact["taxa_alfabetizacao"].isna(), "status_meta_municipio"] = "SEM_RESULTADO"
    have_target = comparable_muni & fact["meta_municipio"].notna() & fact["taxa_alfabetizacao"].notna()
    fact.loc[have_target & (fact["taxa_alfabetizacao"] >= fact["meta_municipio"]), "status_meta_municipio"] = "ATINGIDA"
    fact.loc[have_target & (fact["taxa_alfabetizacao"] < fact["meta_municipio"]), "status_meta_municipio"] = "ABAIXO_DA_META"
    fact["origem_dado"] = "oficial_tabela_agregada"

    uf_mart = silver["uf"].merge(target_uf[["ano", "sigla_uf", "serie", "rede", "meta_uf"]], on=["ano", "sigla_uf", "serie", "rede"], how="left", validate="many_to_one")
    uf_mart = uf_mart.merge(target_br[["ano", "serie", "rede", "meta_brasil"]], on=["ano", "serie", "rede"], how="left", validate="many_to_one")
    uf_mart["gap_meta_uf_pp"] = uf_mart["taxa_alfabetizacao"] - uf_mart["meta_uf"]
    comparable_uf_networks = set(target_uf["rede"].dropna().astype(str))
    comparable_uf = uf_mart["rede"].astype(str).isin(comparable_uf_networks)
    uf_mart["status_meta_uf"] = "NAO_COMPARAVEL"
    uf_mart.loc[comparable_uf, "status_meta_uf"] = "SEM_META"
    uf_mart.loc[comparable_uf & uf_mart["taxa_alfabetizacao"].isna(), "status_meta_uf"] = "SEM_RESULTADO"
    have_uf_target = comparable_uf & uf_mart["meta_uf"].notna() & uf_mart["taxa_alfabetizacao"].notna()
    uf_mart.loc[have_uf_target & (uf_mart["taxa_alfabetizacao"] >= uf_mart["meta_uf"]), "status_meta_uf"] = "ATINGIDA"
    uf_mart.loc[have_uf_target & (uf_mart["taxa_alfabetizacao"] < uf_mart["meta_uf"]), "status_meta_uf"] = "ABAIXO_DA_META"
    uf_mart["origem_dado"] = "oficial_tabela_agregada"

    microdata_indicator = silver["indicador_calculado_microdados"].copy()
    comparable_networks = set(municipality["rede"].dropna().astype(str))
    microdata_indicator = microdata_indicator[microdata_indicator["rede"].astype(str).isin(comparable_networks)]
    reconciliation = microdata_indicator.merge(
        municipality[["ano", "id_municipio", "sigla_uf", "serie", "rede", "taxa_alfabetizacao"]].rename(columns={"taxa_alfabetizacao": "taxa_oficial_publicada"}),
        on=["ano", "id_municipio", "sigla_uf", "serie", "rede"], how="left", validate="one_to_one", indicator=True,
    )
    reconciliation["diferenca_pontos_percentuais"] = reconciliation["taxa_calculada_microdados"] - reconciliation["taxa_oficial_publicada"]
    reconciliation["origem_dado"] = "calculado_microdados_vs_publicado"

    evolution = municipality.groupby(["ano", "serie", "rede"], dropna=False).agg(
        media_simples_municipal_percentual=("taxa_alfabetizacao", "mean"),
        municipios_com_resultado=("id_municipio", "nunique"),
    ).reset_index()
    evolution["indicador_agregado"] = "media_simples_municipal_nao_oficial"
    evolution["origem_dado"] = "oficial_tabela_agregada_agregada_sem_ponderacao"

    stream = silver.get("streaming_events", pd.DataFrame()).copy()
    if not stream.empty:
        stream["origem_dado"] = stream.get("origem_dado", "simulado")
        stream["status_evento"] = "SIMULADO_NAO_OFICIAL"

    return {
        "fato_alfabetizacao_municipio": fact,
        "mart_alfabetizacao_uf": uf_mart,
        "mart_evolucao_temporal": evolution,
        "reconciliacao_microdados": reconciliation,
        "mart_streaming_eventos": stream,
    }
