from pathlib import Path

import pandas as pd

from alfabetiza_pipeline.transform import _apply_dictionary_labels, load_events, normalize_table, transform_to_silver

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
