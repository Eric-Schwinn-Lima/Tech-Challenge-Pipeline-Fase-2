from pathlib import Path

import pandas as pd
import pytest

from alfabetiza_pipeline.quality import QualityGateFailure, enforce_gate, validate_data
from alfabetiza_pipeline.transform import transform_to_silver

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "tests" / "fixtures" / "source"
EVENTS = ROOT / "tests" / "fixtures" / "events" / "stream_events.jsonl"


def _silver():
    raw = {name: pd.read_csv(SOURCE / f"{name}.csv", dtype=str) for name in [
        "uf", "municipio", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf",
        "meta_alfabetizacao_municipio", "alunos",
    ]}
    return transform_to_silver(raw, EVENTS)


def test_quality_gate_passes_on_canonical_demo_data(tmp_path):
    results = validate_data(_silver(), "test-pass", tmp_path)
    assert not [r for r in results if r["severity"] == "BLOCKING" and r["status"] == "FAIL"]
    enforce_gate(results)
    assert (tmp_path / "quality_test-pass.json").exists()


def test_quality_gate_blocks_duplicate_natural_key(tmp_path):
    silver = _silver()
    silver["municipio"] = pd.concat([silver["municipio"], silver["municipio"].iloc[[0]]], ignore_index=True)
    results = validate_data(silver, "test-fail", tmp_path)
    assert any(r["check_name"] == "duplicate_natural_keys:municipio" and r["status"] == "FAIL" for r in results)
    with pytest.raises(QualityGateFailure):
        enforce_gate(results)


def test_quality_gate_blocks_bad_join_rate(tmp_path):
    silver = _silver()
    silver["municipio"].loc[0, "id_municipio"] = "9999999"
    results = validate_data(silver, "test-join-fail", tmp_path)
    check = next(r for r in results if r["check_name"] == "join_match:alunos_to_municipio")
    assert check["status"] == "FAIL"
    with pytest.raises(QualityGateFailure):
        enforce_gate(results)
