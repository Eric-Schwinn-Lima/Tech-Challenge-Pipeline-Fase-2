from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_run_id() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ") + "_" + __import__("uuid").uuid4().hex[:8]


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo obrigatório não encontrado: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=True)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def write_parquet(df: pd.DataFrame, path: Path) -> Path:
    """Write Snappy Parquet when PyArrow/Fastparquet is installed.

    The fallback is a gzip-compressed CSV with an explicit .csv.gz suffix; it never
    masquerades as Parquet. The packaged environment installs PyArrow by default.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False, compression="snappy")
        return path
    except (ImportError, ModuleNotFoundError):
        fallback = path.with_suffix(".csv.gz")
        df.to_csv(fallback, index=False, encoding="utf-8", compression="gzip")
        return fallback


def copy_file_preserving_history(source: Path, destination: Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return destination.stat().st_size
