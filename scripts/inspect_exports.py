from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "exports_raw"
if not RAW_DIR.exists():
    raise SystemExit(f"Crie {RAW_DIR} e coloque ali os CSVs baixados da Base dos Dados.")
for path in sorted(RAW_DIR.glob("*.csv")):
    try:
        frame = pd.read_csv(path, nrows=5, dtype=str)
        print(f"\n## {path.name}\nLinhas de amostra: {len(frame)}\nColunas: {list(frame.columns)}")
        print(frame.head(3).to_string(index=False))
    except Exception as exc:
        print(f"[ERRO] {path.name}: {exc}")
