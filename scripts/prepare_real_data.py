"""Prepare the supplied Base dos Dados CSV exports for the canonical pipeline.

Expected raw exports under data/exports_raw:
  alunos.csv, municipio.csv, uf.csv, meta_alfabetizacao_brasil.csv,
  meta_alfabetizacao_uf.csv, meta_alfabetizacao_municipio.csv

If only dados_oficiais_base_dados.zip is present, this script extracts and renames the
six user-supplied CSVs first. It preserves extracted raw exports and writes canonical
files to data/source.
The official dicionario.csv is supplied beside the ZIP in data/exports_raw. It is
preserved as raw input, copied to data/source, and used to resolve category labels.
"""
from __future__ import annotations

import json
import sys
import zipfile
from shutil import copyfileobj
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "exports_raw"
SOURCE_DIR = ROOT / "data" / "source"

# IBGE municipality-code prefixes (the first two digits) -> UF abbreviation.
# This is a deterministic derivation because the municipal and student exports do
# not carry sigla_uf; the raw ID is preserved in every input file.
UF_BY_IBGE_PREFIX = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}

# The metadata tables use labels (Municipal/Pública), whereas aggregated tables use
# numeric keys. Resolve keys from the official dictionary. The source calls code 5
# "Pública (Estadual e Municipal)"; the short label "Pública" is mapped to that entry
# and this semantic choice is recorded in the inventory. Cross-table rate checks
# support it, but the dictionary does not contain a literal value exactly "Pública".
TARGET_YEARS = range(2024, 2031)


def _extract_supplied_archive_if_needed() -> None:
    """Extract the included user-supplied ZIP when raw CSVs are not yet present."""
    expected = [
        "alunos.csv", "municipio.csv", "uf.csv", "meta_alfabetizacao_brasil.csv",
        "meta_alfabetizacao_uf.csv", "meta_alfabetizacao_municipio.csv",
    ]
    if all((RAW_DIR / name).exists() for name in expected):
        return
    archive = RAW_DIR / "dados_oficiais_base_dados.zip"
    if not archive.exists():
        return
    source_to_target = {
        "alunos.csv": "alunos.csv",
        "br_inep_avaliacao_alfabetizacao_municipio.csv": "municipio.csv",
        "br_inep_avaliacao_alfabetizacao_uf (1).csv": "uf.csv",
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv": "meta_alfabetizacao_brasil.csv",
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv": "meta_alfabetizacao_uf.csv",
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv": "meta_alfabetizacao_municipio.csv",
    }
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as zipped:
        names = set(zipped.namelist())
        for source_name, target_name in source_to_target.items():
            if source_name not in names:
                raise FileNotFoundError(f"O arquivo compactado não contém {source_name}.")
            with zipped.open(source_name) as src, (RAW_DIR / target_name).open("wb") as dst:
                copyfileobj(src, dst, length=1024 * 1024)
            print(f"[extract] {source_name} -> data/exports_raw/{target_name}")


def _require_files() -> None:
    expected = [
        "alunos.csv", "municipio.csv", "uf.csv", "meta_alfabetizacao_brasil.csv",
        "meta_alfabetizacao_uf.csv", "meta_alfabetizacao_municipio.csv", "dicionario.csv",
    ]
    missing = [name for name in expected if not (RAW_DIR / name).exists()]
    if missing:
        raise FileNotFoundError(f"Arquivos brutos obrigatórios ausentes em {RAW_DIR}: {missing}")


def _normalize_dictionary_text(value: object) -> str:
    import unicodedata
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    return " ".join(text.strip().lower().split())


def _load_meta_network_crosswalk() -> dict[str, str]:
    """Create the metadata-label -> numeric-code map from the official dictionary."""
    path = RAW_DIR / "dicionario.csv"
    if not path.exists():
        raise FileNotFoundError(f"Dicionário oficial ausente: {path}")
    dictionary = pd.read_csv(path, dtype={"id_tabela": "string", "nome_coluna": "string", "chave": "string", "valor": "string"})
    required = {"id_tabela", "nome_coluna", "chave", "valor"}
    if not required.issubset(dictionary.columns):
        raise ValueError(f"dicionario.csv precisa conter {sorted(required)}")
    network = dictionary[
        dictionary["id_tabela"].astype("string").str.strip().isin(["municipio", "uf"])
        & dictionary["nome_coluna"].astype("string").str.strip().str.lower().eq("rede")
    ].copy()
    network["valor_norm"] = network["valor"].map(_normalize_dictionary_text)
    municipal = network[network["valor_norm"].eq("municipal")]["chave"].astype("string").str.strip().unique().tolist()
    public = network[network["valor_norm"].eq("publica (estadual e municipal)")]["chave"].astype("string").str.strip().unique().tolist()
    if len(set(municipal)) != 1 or len(set(public)) != 1:
        raise ValueError(
            "Não foi possível derivar as redes das metas de forma unívoca do dicionário: "
            f"Municipal={municipal}, Pública (Estadual e Municipal)={public}"
        )
    return {"municipal": municipal[0], "publica": public[0], "pública": public[0]}


def _municipality_to_uf(values: pd.Series) -> pd.Series:
    ids = values.astype("string").str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(7)
    prefixes = ids.str.slice(0, 2)
    uf = prefixes.map(UF_BY_IBGE_PREFIX).astype("string")
    unknown = ids.notna() & uf.isna()
    if unknown.any():
        examples = ids[unknown].head(10).tolist()
        raise ValueError(f"Não foi possível derivar UF a partir do prefixo IBGE. Exemplos: {examples}")
    return uf


def _write_targets(source_name: str, entity_keys: list[str], target_network: str) -> dict[str, object]:
    path = RAW_DIR / source_name
    raw = pd.read_csv(path, dtype={"id_municipio": "string", "sigla_uf": "string", "rede": "string"}, low_memory=False)
    raw["ano"] = pd.to_numeric(raw["ano"], errors="coerce")
    # Use the latest available metadata snapshot for each entity. Keep all target-year
    # columns in long form, with the snapshot year retained for lineage.
    raw = raw.sort_values("ano").copy()
    if entity_keys:
        raw = raw.drop_duplicates(subset=entity_keys, keep="last").copy()
    else:
        raw = raw.tail(1).copy()
    network_crosswalk = _load_meta_network_crosswalk()
    network_labels = raw["rede"].astype("string").str.strip().str.lower().dropna().unique().tolist()
    mapped = set()
    for label in network_labels:
        if label not in network_crosswalk:
            raise ValueError(f"Rótulo de rede não mapeado em {source_name}: {label!r}. Confira o dicionário oficial.")
        mapped.add(network_crosswalk[label])
    if len(mapped) != 1:
        raise ValueError(f"Mais de uma categoria de rede após o mapeamento em {source_name}: {sorted(mapped)}")

    records: list[pd.DataFrame] = []
    for target_year in TARGET_YEARS:
        col = f"meta_alfabetizacao_{target_year}"
        if col not in raw.columns:
            raise ValueError(f"Coluna esperada ausente: {col} em {source_name}")
        frame = raw[entity_keys + ["ano", col]].copy()
        frame = frame.rename(columns={"ano": "meta_snapshot_ano", col: "meta_alfabetizacao"})
        frame["ano"] = target_year
        frame["serie"] = "2"  # O indicador do desafio é do final do 2º ano fundamental.
        frame["rede"] = next(iter(mapped))
        records.append(frame)
    out = pd.concat(records, ignore_index=True)
    ordered = ["ano", *entity_keys, "serie", "rede", "meta_alfabetizacao", "meta_snapshot_ano"]
    out = out[ordered]
    target_path = SOURCE_DIR / source_name
    target_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(target_path, index=False, encoding="utf-8")
    return {"source_file": source_name, "source_rows": int(len(pd.read_csv(path, usecols=["ano"]))),
            "latest_snapshot_years": sorted(map(int, raw["ano"].dropna().unique())),
            "canonical_rows": int(len(out)), "target_years": [int(x) for x in TARGET_YEARS],
            "network_code_used": next(iter(mapped)), "output": str(target_path.relative_to(ROOT))}


def _prepare_students() -> dict[str, object]:
    source = RAW_DIR / "alunos.csv"
    target = SOURCE_DIR / "alunos.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    seen_years: set[int] = set()
    prefixes: set[str] = set()
    output_columns: list[str] | None = None
    # Chunking avoids loading the one-million-row microdata table into memory all at once.
    for chunk_idx, chunk in enumerate(pd.read_csv(source, dtype={"id_municipio": "string", "id_aluno": "string", "rede": "string", "serie": "string"}, chunksize=150_000, low_memory=False)):
        chunk.columns = [str(c).strip().lower() for c in chunk.columns]
        required = {"ano", "id_municipio", "id_aluno", "serie", "rede", "presenca", "alfabetizado", "peso_aluno"}
        missing = sorted(required - set(chunk.columns))
        if missing:
            raise ValueError(f"Colunas obrigatórias ausentes em alunos.csv: {missing}")
        chunk["id_municipio"] = chunk["id_municipio"].astype("string").str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(7)
        chunk["sigla_uf"] = _municipality_to_uf(chunk["id_municipio"])
        chunk = chunk.rename(columns={"peso_aluno": "peso_amostral"})
        chunk["origem_dado"] = "oficial_microdados_fornecidos"
        chunk["ano"] = pd.to_numeric(chunk["ano"], errors="coerce").astype("Int64")
        seen_years.update(int(v) for v in chunk["ano"].dropna().unique())
        prefixes.update(chunk["id_municipio"].str[:2].dropna().unique().tolist())
        if output_columns is None:
            output_columns = list(chunk.columns)
            chunk.to_csv(target, index=False, mode="w", header=True, encoding="utf-8")
        else:
            # Keep the initial column order to ensure stable CSV schema.
            chunk = chunk[output_columns]
            chunk.to_csv(target, index=False, mode="a", header=False, encoding="utf-8")
        total_rows += len(chunk)
        print(f"[alunos] bloco {chunk_idx + 1}: {len(chunk):,} linhas; acumulado {total_rows:,}")

    return {"source_file": "alunos.csv", "source_rows": total_rows, "years": sorted(seen_years),
            "municipality_prefixes": sorted(prefixes), "canonical_rows": total_rows,
            "output": str(target.relative_to(ROOT)), "chunk_size": 150_000}


def _prepare_simple_table(source_name: str, table: str, derive_uf: bool = False) -> dict[str, object]:
    path = RAW_DIR / source_name
    df = pd.read_csv(path, dtype={"id_municipio": "string", "sigla_uf": "string", "rede": "string", "serie": "string"}, low_memory=False)
    df.columns = [str(c).strip().lower() for c in df.columns]
    if derive_uf:
        if "id_municipio" not in df.columns:
            raise ValueError(f"Tabela {table} não tem id_municipio para derivar a UF.")
        df["id_municipio"] = df["id_municipio"].astype("string").str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(7)
        df["sigla_uf"] = _municipality_to_uf(df["id_municipio"])
    target = SOURCE_DIR / f"{table}.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False, encoding="utf-8")
    return {"source_file": source_name, "source_rows": int(len(df)), "source_columns": list(df.columns),
            "canonical_rows": int(len(df)), "output": str(target.relative_to(ROOT))}


def _prepare_dictionary() -> dict[str, object]:
    source = RAW_DIR / "dicionario.csv"
    df = pd.read_csv(source, dtype={"id_tabela": "string", "nome_coluna": "string", "chave": "string", "cobertura_temporal": "string", "valor": "string"})
    df.columns = [str(c).strip().lower() for c in df.columns]
    required = {"id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"dicionario.csv sem colunas obrigatórias: {missing}")
    for col in ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]:
        df[col] = df[col].astype("string").str.strip()
    if df.duplicated(["id_tabela", "nome_coluna", "chave"]).any():
        raise ValueError("dicionario.csv possui chaves repetidas para id_tabela/nome_coluna/chave")
    target = SOURCE_DIR / "dicionario.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False, encoding="utf-8")
    return {"source_file": "dicionario.csv", "source_rows": int(len(df)), "source_columns": list(df.columns),
            "canonical_rows": int(len(df)), "output": str(target.relative_to(ROOT))}


def main() -> None:
    _extract_supplied_archive_if_needed()
    _require_files()
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    inventory: dict[str, object] = {
        "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "Base dos Dados — br_inep_avaliacao_alfabetizacao",
        "note": "A tabela oficial dicionario foi preservada em Bronze e copiada para a fonte canônica; as descrições categóricas são derivadas dela.",
        "metadata_network_crosswalk": _load_meta_network_crosswalk(),
        "files": {},
        "limitations": [],
    }
    inventory["files"]["dicionario"] = _prepare_dictionary()
    inventory["files"]["uf"] = _prepare_simple_table("uf.csv", "uf")
    inventory["files"]["municipio"] = _prepare_simple_table("municipio.csv", "municipio", derive_uf=True)
    inventory["files"]["meta_alfabetizacao_brasil"] = _write_targets("meta_alfabetizacao_brasil.csv", [], "5")
    inventory["files"]["meta_alfabetizacao_uf"] = _write_targets("meta_alfabetizacao_uf.csv", ["sigla_uf"], "5")
    inventory["files"]["meta_alfabetizacao_municipio"] = _write_targets("meta_alfabetizacao_municipio.csv", ["id_municipio"], "3")
    inventory["files"]["alunos"] = _prepare_students()

    student_rows = inventory["files"]["alunos"]["source_rows"]
    # 1,048,575 records is a strong warning sign of the Excel worksheet row limit
    # (1,048,576 rows including the header); this is a warning, not proof of truncation.
    if student_rows == 1_048_575:
        warning = ("alunos.csv contém exatamente 1.048.575 registros, padrão compatível com limite de planilha "
                   "(1.048.576 linhas incluindo cabeçalho). Confirme a completude via BigQuery/pacote Base dos Dados; "
                   "a reconciliação com microdados não deve ser declarada conclusiva até essa verificação.")
        inventory["limitations"].append(warning)
        print("[ATENÇÃO] " + warning)

    inv_path = ROOT / "data" / "exports_raw" / "source_inventory.json"
    inv_path.write_text(json.dumps(inventory, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nInventário salvo em {inv_path.relative_to(ROOT)}")
    print("Fontes canônicas geradas em data/source/. Execute: python scripts/validate_sources.py")


if __name__ == "__main__":
    main()
