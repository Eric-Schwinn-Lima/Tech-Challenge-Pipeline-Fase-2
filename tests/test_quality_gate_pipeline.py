from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from alfabetiza_pipeline import pipeline as pipeline_module
from alfabetiza_pipeline.quality import QualityGateFailure, validate_data
from alfabetiza_pipeline.transform import transform_to_silver

ROOT = Path(__file__).resolve().parents[1]
SOURCE_FIXTURES = ROOT / "tests" / "fixtures" / "source"
EVENTS_FIXTURE = ROOT / "tests" / "fixtures" / "events" / "stream_events.jsonl"


def test_pipeline_blocks_silver_and_gold_when_quality_gate_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A duplicate municipal natural key must fail the real gate before publication."""
    run_id = "test-quality-gate-blocking"
    data_dir = tmp_path / "data"
    source_dir = data_dir / "source"
    observability_dir = data_dir / "observability"

    shutil.copytree(SOURCE_FIXTURES, source_dir)

    # Redirect all local pipeline outputs to an isolated temporary directory.
    monkeypatch.setattr(pipeline_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(pipeline_module, "SOURCE_DIR", source_dir)
    monkeypatch.setattr(pipeline_module, "EVENTS_FILE", EVENTS_FIXTURE)
    monkeypatch.setattr(pipeline_module, "OBSERVABILITY_DIR", observability_dir)
    monkeypatch.setattr(pipeline_module, "make_run_id", lambda: run_id)

    # Inject a deterministic data defect after normal Silver transformation.
    original_transform = pipeline_module.transform_to_silver

    def transform_with_duplicate(raw_tables, events_file):
        silver = original_transform(raw_tables, events_file)
        municipality = silver["municipio"]
        assert not municipality.empty, "Municipality fixture must contain at least one row"

        silver["municipio"] = pd.concat(
            [municipality, municipality.iloc[[0]]],
            ignore_index=True,
        )
        return silver

    monkeypatch.setattr(
        pipeline_module,
        "transform_to_silver",
        transform_with_duplicate,
    )

    # The real validator must detect the duplicate and the orchestration must re-raise.
    with pytest.raises(QualityGateFailure, match="Quality gate bloqueou"):
        pipeline_module.run_pipeline()

    # Quality evidence survives the failure.
    report_path = observability_dir / f"quality_{run_id}.json"
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["blocking_failures"] >= 1

    duplicate_check = next(
        check
        for check in report["checks"]
        if check["check_name"] == "duplicate_natural_keys:municipio"
    )
    assert duplicate_check["severity"] == "BLOCKING"
    assert duplicate_check["status"] == "FAIL"
    assert duplicate_check["measured"] >= 1

    # The pipeline records its terminal status and the failed Quality Gate stage.
    summary_path = observability_dir / f"run_summary_{run_id}.json"
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "FAILED"
    assert summary["error_type"] == "QualityGateFailure"

    metrics_path = observability_dir / "run_metrics.jsonl"
    metrics = [
        json.loads(line)
        for line in metrics_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    quality_events = [
        event for event in metrics if event.get("stage") == "quality_gate"
    ]
    assert quality_events
    assert quality_events[-1]["status"] == "FAILED"
    assert any(
        event.get("stage") == "pipeline_end" and event.get("status") == "FAILED"
        for event in metrics
    )

    # Critical assertion: neither downstream layer may be published.
    assert not (data_dir / "silver" / f"run_id={run_id}").exists()
    assert not (data_dir / "gold" / f"run_id={run_id}").exists()
    assert not (data_dir / "silver" / f"_staging_run_id={run_id}").exists()
    assert not (data_dir / "gold" / f"_staging_run_id={run_id}").exists()

    # The defect is reported, but source fixtures remain untouched.
    assert (source_dir / "municipio.csv").exists()


def test_quality_gate_rejects_non_binary_flags_and_invalid_proficiency_percentages(tmp_path: Path) -> None:
    raw = {
        name: pd.read_csv(SOURCE_FIXTURES / f"{name}.csv", dtype=str)
        for name in [
            "uf", "municipio", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf",
            "meta_alfabetizacao_municipio", "alunos",
        ]
    }
    raw["alunos"].loc[0, "presenca"] = "1.5"
    raw["municipio"]["proporcao_aluno_nivel_0"] = 120.0
    silver = transform_to_silver(raw, EVENTS_FIXTURE)

    results = validate_data(silver, "test-domain-rules", tmp_path / "observability")
    by_name = {item["check_name"]: item for item in results}

    assert by_name["binary_domain:alunos.presenca"]["status"] == "FAIL"
    assert by_name["binary_domain:alunos.presenca"]["severity"] == "BLOCKING"
    assert by_name["proficiency_level_range:municipio"]["status"] == "FAIL"
    assert by_name["proficiency_level_range:municipio"]["severity"] == "BLOCKING"
