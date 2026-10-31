from pathlib import Path

import pandas as pd

from alfabetiza_pipeline.gold_metrics import build_proficiency_profile
from alfabetiza_pipeline.transform import _apply_dictionary_labels, build_gold, load_events, normalize_table, transform_to_silver

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "tests" / "fixtures" / "source"
EVENTS = ROOT / "tests" / "fixtures" / "events" / "stream_events.jsonl"


def test_normalize_table_preserves_municipality_code_as_string():
    source = pd.read_csv(SOURCE / "municipio.csv", dtype=str)
    normalized = normalize_table("municipio", source)
    assert normalized["id_municipio"].dtype.name == "string"
    assert normalized.loc[0, "id_municipio"] == "4300001"
    assert normalized["taxa_alfabetizacao"].between(0, 100).all()


def test_weighted_student_indicator_uses_present_students_and_sample_weight():
    raw = {name: pd.read_csv(SOURCE / f"{name}.csv", dtype=str) for name in [
        "uf", "municipio", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf",
        "meta_alfabetizacao_municipio", "alunos",
    ]}
    silver = transform_to_silver(raw, EVENTS)
    result = silver["indicador_calculado_microdados"]
    record = result[(result["ano"] == 2023) & (result["id_municipio"] == "4300001")].iloc[0]
    assert round(float(record["taxa_calculada_microdados"]), 2) == 60.0
    assert int(record["alunos_avaliados"]) == 5


def test_streaming_duplicate_is_removed_and_invalid_event_quarantined():
    valid, quarantine, duplicates = load_events(EVENTS)
    assert len(valid) == 2
    assert len(quarantine) == 1
    assert duplicates == 1
    assert valid["origem_dado"].eq("simulado").all()
    assert valid["latencia_segundos"].notna().all()


def test_official_dictionary_adds_descriptions_without_replacing_codes():
    dictionary = normalize_table("dicionario", pd.read_csv(SOURCE / "dicionario.csv", dtype=str))
    municipality = pd.DataFrame({
        "ano": [2024], "id_municipio": ["4300001"], "sigla_uf": ["RS"],
        "serie": ["2"], "rede": ["5"], "taxa_alfabetizacao": [80.0],
    })
    municipality = normalize_table("municipio", municipality)
    result = _apply_dictionary_labels({"municipio": municipality, "dicionario": dictionary})["municipio"]
    assert result.loc[0, "rede"] == "5"  # join key remains unchanged
    assert result.loc[0, "rede_descricao"] == "Pública (Estadual e Municipal)"
    assert result.loc[0, "serie_descricao"] == "2° ano do Ensino Fundamental"


def test_gold_builds_target_monitoring_and_trajectory_marts():
    raw = {name: pd.read_csv(SOURCE / f"{name}.csv", dtype=str) for name in [
        "uf", "municipio", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf",
        "meta_alfabetizacao_municipio", "alunos",
    ]}
    silver = transform_to_silver(raw, EVENTS)
    gold = build_gold(silver)

    assert {
        "mart_perfil_proficiencia",
        "mart_acompanhamento_metas",
        "mart_trajetoria_metas",
        "mart_evolucao_temporal",
    }.issubset(gold)

    trajectory = gold["mart_trajetoria_metas"]
    target = trajectory[
        (trajectory["nivel_geografico"] == "MUNICIPIO")
        & (trajectory["id_municipio"] == "4300001")
        & (trajectory["ano_meta"] == 2025)
    ].iloc[0]
    assert float(target["variacao_meta_pp_ano_anterior"]) == 3.0

    monitoring = gold["mart_acompanhamento_metas"]
    municipal_rs_2024 = monitoring[
        (monitoring["nivel_geografico"] == "MUNICIPIOS_POR_UF")
        & (monitoring["sigla_uf"] == "RS")
        & (monitoring["ano"] == 2024)
    ].iloc[0]
    assert int(municipal_rs_2024["total_registros"]) == 2
    assert int(municipal_rs_2024["atingida"]) == 2
    assert int(municipal_rs_2024["abaixo_da_meta"]) == 0
    assert round(float(municipal_rs_2024["percentual_atingimento_entre_comparaveis"]), 2) == 100.0

    evolution = gold["mart_evolucao_temporal"]
    current = evolution[(evolution["ano"] == 2024) & (evolution["rede"] == "publica")].iloc[0]
    assert int(current["municipios_comparaveis_ano_anterior"]) == 3
    assert int(current["municipios_melhoraram"]) == 2
    assert int(current["municipios_pioraram"]) == 0
    assert int(current["municipios_estaveis"]) == 1


def test_proficiency_profile_unpivots_levels_without_recomputing_official_rate():
    municipality = pd.DataFrame({
        "ano": [2024],
        "id_municipio": ["4300001"],
        "sigla_uf": ["RS"],
        "serie": ["2"],
        "rede": ["publica"],
        "taxa_alfabetizacao": [72.0],
        "media_portugues": [711.5],
        **{f"proporcao_aluno_nivel_{level}": [10.0 if level == 0 else 0.0] for level in range(9)},
    })
    profile = build_proficiency_profile(municipality, pd.DataFrame())
    assert len(profile) == 9
    assert profile["proporcao_alunos_pct"].sum() == 10.0
    assert profile["soma_niveis_pct"].eq(10.0).all()
    assert profile["taxa_alfabetizacao"].eq(72.0).all()
    assert profile.loc[profile["nivel_desempenho"] == 0, "proporcao_alunos_pct"].iloc[0] == 10.0
