from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from alfabetiza_pipeline.config import SOURCE_DIR, TABLES  # noqa: E402
from alfabetiza_pipeline.transform import normalize_table  # noqa: E402
from alfabetiza_pipeline.io_utils import read_csv  # noqa: E402

errors = []
for table in TABLES:
    path = SOURCE_DIR / f"{table}.csv"
    try:
        frame = read_csv(path)
        normalized = normalize_table(table, frame)
        print(f"[OK] {table}: {len(normalized)} linhas; colunas={len(normalized.columns)}")
    except Exception as exc:
        errors.append(f"{table}: {exc}")
        print(f"[ERRO] {table}: {exc}")
dictionary_path = SOURCE_DIR / "dicionario.csv"
if dictionary_path.exists():
    try:
        dictionary = read_csv(dictionary_path)
        normalized = normalize_table("dicionario", dictionary)
        print(f"[OK] dicionario: {len(normalized)} linhas; colunas={len(normalized.columns)}; usado como referência categórica")
    except Exception as exc:
        errors.append(f"dicionario: {exc}")
        print(f"[ERRO] dicionario: {exc}")
else:
    print("[AVISO] data/source/dicionario.csv não encontrado. O pipeline local pode rodar sem rótulos descriptivos, mas a entrega com dados oficiais deve incluir o dicionário.")
if errors:
    raise SystemExit(1)
print("As seis tabelas obrigatórias foram reconhecidas; o dicionário auxiliar foi validado quando disponível.")
