from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .config import DATA_DIR, EVENTS_FILE, OBSERVABILITY_DIR, SOURCE_DIR, TABLES
from .io_utils import append_jsonl, copy_file_preserving_history, iso_now, make_run_id, read_csv, write_csv, write_parquet
from .quality import QualityGateFailure, enforce_gate, validate_data
from .transform import build_gold, transform_to_silver


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_stage(
    metrics_path: Path,
    run_id: str,
    stage: str,
    action,
    rows_in: int = 0,
) -> Any:
    started_at = iso_now()
    start = time.perf_counter()
    status = "SUCCESS"
    details: dict[str, Any] = {}
    rows_out = 0
    bytes_out = 0
    try:
        result = action()
        if isinstance(result, dict):
            rows_out = sum(len(v) for v in result.values() if isinstance(v, pd.DataFrame))
        elif isinstance(result, pd.DataFrame):
            rows_out = len(result)
        elif isinstance(result, int):
            rows_out = result
        elif isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], dict):
            stats = result[1]
            rows_out = sum(int(item.get("rows", 0)) for item in stats.values() if isinstance(item, dict))
            bytes_out = sum(int(item.get("columnar_bytes", 0)) for item in stats.values() if isinstance(item, dict))
        details["rows_in"] = int(rows_in)
        if isinstance(result, dict):
            details["tables"] = {k: len(v) for k, v in result.items() if isinstance(v, pd.DataFrame)}
        if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], dict):
            details["tables"] = {k: v.get("rows", 0) for k, v in result[1].items() if isinstance(v, dict)}
            details["storage_formats"] = sorted({v.get("storage_format", "unknown") for v in result[1].values() if isinstance(v, dict)})
        return result
    except Exception as exc:
        status = "FAILED"
        details = {"error": str(exc), "error_type": type(exc).__name__, "rows_in": int(rows_in)}
        raise
    finally:
        duration = round(time.perf_counter() - start, 6)
        append_jsonl(metrics_path, {
            "run_id": run_id,
            "stage": stage,
            "status": status,
            "started_at": started_at,
            "finished_at": iso_now(),
            "duration_seconds": duration,
            "rows_in": int(rows_in),
            "rows_out": int(rows_out),
            "bytes_out": int(bytes_out),
            "details": details,
        })


def _ingest_batch(run_id: str) -> tuple[Path, dict[str, int]]:
    missing = [name for name in TABLES if not (SOURCE_DIR / f"{name}.csv").exists()]
    if missing:
        raise FileNotFoundError(f"Fontes obrigatórias ausentes em {SOURCE_DIR}: {missing}")
    run_root = DATA_DIR / "bronze" / f"run_id={run_id}"
    batch_root = run_root / "batch"
    sizes: dict[str, int] = {}
    raw_exports_dir = DATA_DIR / "exports_raw"
    manifest = {"run_id": run_id, "ingested_at": iso_now(), "source_mode": "official_exports_with_canonical_adapter" if raw_exports_dir.exists() else "csv_files", "tables": {}, "raw_exports": {}}
    # Preserve original source exports byte-for-byte in Bronze alongside the canonical
    # tables used by the transformation code. source_inventory.json is metadata, not raw data.
    if raw_exports_dir.exists():
        for raw_source in sorted(raw_exports_dir.glob("*.csv")):
            raw_destination = batch_root / "raw_exports" / raw_source.name
            raw_size = copy_file_preserving_history(raw_source, raw_destination)
            sizes[f"raw:{raw_source.name}"] = raw_size
            manifest["raw_exports"][raw_source.name] = {
                "bronze_file": str(raw_destination.relative_to(DATA_DIR)),
                "bytes": raw_size,
                "rows": max(0, sum(1 for _ in raw_source.open(encoding="utf-8")) - 1),
                "sha256": _sha256(raw_source),
            }
    for table in TABLES:
        source = SOURCE_DIR / f"{table}.csv"
        destination = batch_root / table / "data.csv"
        sizes[table] = copy_file_preserving_history(source, destination)
        manifest["tables"][table] = {
            "source_file": str(source.relative_to(DATA_DIR)) if source.is_relative_to(DATA_DIR) else source.name,
            "bronze_file": str(destination.relative_to(DATA_DIR)),
            "bytes": sizes[table],
            "rows": max(0, sum(1 for _ in source.open(encoding="utf-8")) - 1),
            "sha256": _sha256(source),
        }
    # The dictionary is an auxiliary reference entity, not a substitute for any of the six required business tables.
    dictionary_source = SOURCE_DIR / "dicionario.csv"
    if dictionary_source.exists():
        dictionary_destination = batch_root / "dicionario" / "data.csv"
        sizes["dicionario"] = copy_file_preserving_history(dictionary_source, dictionary_destination)
        manifest["tables"]["dicionario"] = {
            "source_file": str(dictionary_source.relative_to(DATA_DIR)) if dictionary_source.is_relative_to(DATA_DIR) else dictionary_source.name,
            "bronze_file": str(dictionary_destination.relative_to(DATA_DIR)),
            "bytes": sizes["dicionario"],
            "rows": max(0, sum(1 for _ in dictionary_source.open(encoding="utf-8")) - 1),
            "sha256": _sha256(dictionary_source),
            "role": "reference_dictionary_used_to_decode_categories",
        }
    (run_root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return run_root, sizes


def _ingest_stream(run_id: str, run_root: Path) -> tuple[Path, int, int]:
    target_dir = run_root / "streaming" / f"ingest_date={datetime.now(timezone.utc).date().isoformat()}"
    target = target_dir / "events.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    if EVENTS_FILE.exists():
        shutil.copy2(EVENTS_FILE, target)
    else:
        target.write_text("", encoding="utf-8")
    rows = sum(1 for line in target.open(encoding="utf-8") if line.strip())
    return target, rows, target.stat().st_size


def _load_bronze(run_root: Path) -> dict[str, pd.DataFrame]:
    tables = {table: read_csv(run_root / "batch" / table / "data.csv") for table in TABLES}
    dictionary_path = run_root / "batch" / "dicionario" / "data.csv"
    if dictionary_path.exists():
        tables["dicionario"] = read_csv(dictionary_path)
    return tables


def _write_layer(layer: str, run_id: str, tables: dict[str, pd.DataFrame]) -> tuple[Path, dict[str, Any]]:
    staging = DATA_DIR / layer / f"_staging_run_id={run_id}"
    final = DATA_DIR / layer / f"run_id={run_id}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)
    stats: dict[str, Any] = {}
    for name, df in tables.items():
        if not isinstance(df, pd.DataFrame):
            continue
        # Write a single analytical representation to avoid paying for both a full
        # uncompressed CSV and Parquet/compressed fallback. If PyArrow is unavailable,
        # write_parquet creates an explicitly named .csv.gz fallback.
        parquet_path = staging / f"{name}.parquet"
        columnar_path = write_parquet(df, parquet_path)
        stats[name] = {
            "rows": len(df),
            "columnar_bytes": columnar_path.stat().st_size,
            "storage_format": "parquet_snappy" if columnar_path.suffix == ".parquet" else "csv_gzip_fallback",
            "columnar_file": columnar_path.name,
        }
    # Publish atomically after every table has been created successfully. A run-specific path
    # preserves all previous layers rather than overwriting the history.
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.exists():
        raise FileExistsError(f"A camada {layer} para a execução {run_id} já existe; não será sobrescrita.")
    os.replace(staging, final)
    return final, stats


def run_pipeline() -> dict[str, Any]:
    run_id = make_run_id()
    OBSERVABILITY_DIR.mkdir(parents=True, exist_ok=True)
    metrics_path = OBSERVABILITY_DIR / "run_metrics.jsonl"
    summary: dict[str, Any] = {"run_id": run_id, "started_at": iso_now(), "status": "RUNNING", "mode": "local_demo_or_csv"}
    summary_path = OBSERVABILITY_DIR / f"run_summary_{run_id}.json"
    append_jsonl(metrics_path, {"run_id": run_id, "stage": "pipeline_start", "status": "STARTED", "started_at": summary["started_at"], "finished_at": None, "duration_seconds": 0, "rows_in": 0, "rows_out": 0, "bytes_out": 0, "details": {"source_dir": str(SOURCE_DIR), "events_file": str(EVENTS_FILE)}})
    try:
        run_root, size_map = _record_stage(metrics_path, run_id, "ingest_batch", lambda: _ingest_batch(run_id))
        stream_file, event_rows, event_bytes = _record_stage(metrics_path, run_id, "ingest_streaming", lambda: _ingest_stream(run_id, run_root))
        raw_tables = _record_stage(metrics_path, run_id, "load_bronze", lambda: _load_bronze(run_root))
        raw_rows = sum(len(df) for df in raw_tables.values())

        silver = _record_stage(metrics_path, run_id, "transform_silver_in_memory", lambda: transform_to_silver(raw_tables, stream_file), rows_in=raw_rows + event_rows)
        quality = validate_data(silver, run_id, OBSERVABILITY_DIR)
        passed = sum(item["status"] == "PASS" for item in quality)
        failed = sum(item["severity"] == "BLOCKING" and item["status"] == "FAIL" for item in quality)
        append_jsonl(metrics_path, {"run_id": run_id, "stage": "quality_gate", "status": "SUCCESS" if failed == 0 else "FAILED", "started_at": iso_now(), "finished_at": iso_now(), "duration_seconds": 0, "rows_in": raw_rows, "rows_out": passed, "bytes_out": 0, "details": {"checks": len(quality), "blocking_failures": failed, "quality_report": str((OBSERVABILITY_DIR / f"quality_{run_id}.json").relative_to(DATA_DIR))}})
        enforce_gate(quality)

        # Do not publish Silver before its blocking data-quality rules have passed.
        silver_tables = {key: value for key, value in silver.items() if isinstance(value, pd.DataFrame) and not key.startswith("streaming_quarantine")}
        silver_path, silver_stats = _record_stage(metrics_path, run_id, "publish_silver", lambda: _write_layer("silver", run_id, silver_tables), rows_in=raw_rows)
        quarantine = silver.get("streaming_quarantine", pd.DataFrame())
        if not quarantine.empty:
            quarantine_path = DATA_DIR / "quarantine" / f"run_id={run_id}" / "streaming_invalid_events.csv"
            write_csv(quarantine, quarantine_path)
        gold_tables = _record_stage(metrics_path, run_id, "transform_gold_in_memory", lambda: build_gold(silver), rows_in=sum(len(x) for x in silver_tables.values()))
        # Final sanity checks before publishing the Gold layer.
        gold_blocking = []
        for name in ["fato_alfabetizacao_municipio", "mart_alfabetizacao_uf", "mart_evolucao_temporal", "reconciliacao_microdados"]:
            df = gold_tables.get(name, pd.DataFrame())
            if df.empty:
                gold_blocking.append(f"Gold table is empty: {name}")
        fact = gold_tables.get("fato_alfabetizacao_municipio", pd.DataFrame())
        if not fact.empty and not fact["taxa_alfabetizacao"].dropna().between(0, 100).all():
            gold_blocking.append("Gold fact contains rates outside [0, 100]")
        append_jsonl(OBSERVABILITY_DIR / "quality_history.jsonl", {"run_id": run_id, "check_name": "gold_publish_preconditions", "severity": "BLOCKING", "status": "FAIL" if gold_blocking else "PASS", "measured": gold_blocking, "threshold": "all Gold marts non-empty and rates valid", "checked_at": iso_now()})
        if gold_blocking:
            raise QualityGateFailure("Quality gate da Gold bloqueou publicação: " + "; ".join(gold_blocking))
        gold_path, gold_stats = _record_stage(metrics_path, run_id, "publish_gold", lambda: _write_layer("gold", run_id, gold_tables), rows_in=sum(len(x) for x in gold_tables.values()))

        summary.update({
            "status": "SUCCESS",
            "finished_at": iso_now(),
            "source_note": ("As tabelas canônicas em data/source foram preparadas a partir dos CSVs de origem preservados em data/exports_raw; a tabela oficial dicionario é incluída como referência para decodificação categórica; os eventos em data/events continuam sendo simulados. Consulte data/exports_raw/source_inventory.json para as limitações conhecidas, incluindo possível truncamento de alunos." if (DATA_DIR / "exports_raw").exists() else "Os arquivos em data/source são fixtures sintéticas de testes; substitua por exportações oficiais antes de análise substantiva."),
            "bronze_path": str(run_root.relative_to(DATA_DIR)),
            "silver_path": str(silver_path.relative_to(DATA_DIR)),
            "gold_path": str(gold_path.relative_to(DATA_DIR)),
            "source_rows": {name: len(df) for name, df in raw_tables.items()},
            "stream_events_received": event_rows,
            "stream_events_accepted": len(silver.get("streaming_events", pd.DataFrame())),
            "stream_events_quarantined": len(quarantine),
            "stream_events_duplicates_removed": silver.get("streaming_duplicate_count", 0),
            "quality_checks": len(quality),
            "quality_blocking_failures": failed,
            "bronze_bytes": sum(size_map.values()) + event_bytes,
            "silver_stats": silver_stats,
            "gold_stats": gold_stats,
            "gold_rows": {name: len(df) for name, df in gold_tables.items()},
        })
        (OBSERVABILITY_DIR / "last_success.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        summary.update({"status": "FAILED", "finished_at": iso_now(), "error_type": type(exc).__name__, "error": str(exc)})
        append_jsonl(metrics_path, {"run_id": run_id, "stage": "pipeline_end", "status": "FAILED", "started_at": summary["started_at"], "finished_at": summary["finished_at"], "duration_seconds": 0, "rows_in": 0, "rows_out": 0, "bytes_out": 0, "details": {"error_type": type(exc).__name__, "error": str(exc)}})
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        raise
    else:
        append_jsonl(metrics_path, {"run_id": run_id, "stage": "pipeline_end", "status": "SUCCESS", "started_at": summary["started_at"], "finished_at": summary["finished_at"], "duration_seconds": 0, "rows_in": raw_rows, "rows_out": sum(summary["gold_rows"].values()), "bytes_out": sum(s.get("parquet_bytes", 0) for s in summary["gold_stats"].values()), "details": {"gold_path": summary["gold_path"]}})
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary
