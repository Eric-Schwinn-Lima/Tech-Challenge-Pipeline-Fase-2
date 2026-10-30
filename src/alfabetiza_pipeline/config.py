from __future__ import annotations

import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT_DIR / "data"))
SOURCE_DIR = Path(os.getenv("SOURCE_DIR", DATA_DIR / "source"))
EVENTS_FILE = Path(os.getenv("EVENTS_FILE", DATA_DIR / "events" / "stream_events.jsonl"))
OBSERVABILITY_DIR = DATA_DIR / "observability"
MIN_JOIN_MATCH_RATE = float(os.getenv("MIN_JOIN_MATCH_RATE", "0.95"))
# Missing/non-positive sample weights are excluded from the weighted estimate and
# block publication only when they exceed this configured share of present students.
MAX_INVALID_WEIGHT_RATE = float(os.getenv("MAX_INVALID_WEIGHT_RATE", "0.001"))

TABLES = (
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "alunos",
)

KEYS = {
    "uf": ["ano", "sigla_uf", "serie", "rede"],
    "municipio": ["ano", "id_municipio", "serie", "rede"],
    "meta_alfabetizacao_brasil": ["ano", "serie", "rede"],
    "meta_alfabetizacao_uf": ["ano", "sigla_uf", "serie", "rede"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio", "serie", "rede"],
}
