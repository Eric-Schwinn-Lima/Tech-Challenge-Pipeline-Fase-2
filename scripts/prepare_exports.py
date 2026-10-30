"""Generic helper for manually supplied, already-canonical CSV exports.

For the official FIAP challenge data supplied with this repository, prefer
`scripts/prepare_real_data.py`: it extracts the bundled six-table ZIP, requires the
official `dicionario.csv`, derives the metadata network crosswalk from that dictionary,
and produces the canonical source files. This generic helper is kept for alternate
exports; its optional JSON mapping is not the source of truth for the official dataset.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from alfabetiza_pipeline.config import SOURCE_DIR, TABLES  # noqa: E402
from alfabetiza_pipeline.transform import canonicalize_columns, normalize_table  # noqa: E402

RAW_DIR = ROOT / "data" / "exports_raw"
MAPPING_FILE = ROOT / "config" / "category_mappings.json"
if not RAW_DIR.exists():
    raise SystemExit(f"Diretório não encontrado: {RAW_DIR}")

mappings = {}
if MAPPING_FILE.exists():
    mappings = json.loads(MAPPING_FILE.read_text(encoding="utf-8"))

failures = []
for table in TABLES:
    path = RAW_DIR / f"{table}.csv"
    if not path.exists():
        failures.append(f"Arquivo obrigatório ausente: {path}")
        continue
    try:
        raw = pd.read_csv(path, dtype=str, keep_default_na=True)
        canonical = canonicalize_columns(raw, table)
        for column, value_map in mappings.items():
            if column in canonical.columns and isinstance(value_map, dict):
                canonical[column] = canonical[column].astype("string").map(lambda v: value_map.get(str(v), v) if pd.notna(v) else v)
        normalized = normalize_table(table, canonical)
        target = SOURCE_DIR / f"{table}.csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_csv(target, index=False, encoding="utf-8")
        print(f"[OK] {table}: {len(normalized):,} linhas -> {target.relative_to(ROOT)}")
    except Exception as exc:
        failures.append(f"{table}: {exc}")
        print(f"[ERRO] {table}: {exc}")

if failures:
    raise SystemExit("\nNão foi possível preparar todas as tabelas:\n- " + "\n- ".join(failures))
print("Fontes preparadas. Execute 'make validate' antes do pipeline.")
