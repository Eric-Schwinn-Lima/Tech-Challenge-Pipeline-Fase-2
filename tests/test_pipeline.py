import json
import shutil
from pathlib import Path

import pytest

from alfabetiza_pipeline import pipeline as pipeline_module

ROOT = Path(__file__).resolve().parents[1]


def test_end_to_end_pipeline_publishes_bronze_silver_gold(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    source_dir = data_dir / "source"
    events_dir = data_dir / "events"
    shutil.copytree(ROOT / "tests" / "fixtures" / "source", source_dir)
    shutil.copytree(ROOT / "tests" / "fixtures" / "events", events_dir)
    obs_dir = data_dir / "observability"
    monkeypatch.setattr(pipeline_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(pipeline_module, "SOURCE_DIR", source_dir)
    monkeypatch.setattr(pipeline_module, "EVENTS_FILE", events_dir / "stream_events.jsonl")
    monkeypatch.setattr(pipeline_module, "OBSERVABILITY_DIR", obs_dir)

    result = pipeline_module.run_pipeline()
    assert result["status"] == "SUCCESS"
    assert result["quality_blocking_failures"] == 0
    assert (data_dir / result["bronze_path"] / "manifest.json").exists()
    assert ((data_dir / result["silver_path"] / "municipio.parquet").exists() or (data_dir / result["silver_path"] / "municipio.csv.gz").exists())
    assert ((data_dir / result["gold_path"] / "fato_alfabetizacao_municipio.parquet").exists() or (data_dir / result["gold_path"] / "fato_alfabetizacao_municipio.csv.gz").exists())
    assert (obs_dir / "run_metrics.jsonl").exists()
    assert result["stream_events_accepted"] == 2
    assert result["stream_events_quarantined"] == 1
    saved = json.loads((obs_dir / f"run_summary_{result['run_id']}.json").read_text(encoding="utf-8"))
    assert saved["status"] == "SUCCESS"
