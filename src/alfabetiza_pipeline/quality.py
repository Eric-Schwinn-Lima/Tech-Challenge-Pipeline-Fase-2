from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config import MAX_INVALID_WEIGHT_RATE, MIN_JOIN_MATCH_RATE, TABLES
from .io_utils import append_jsonl, iso_now


class QualityGateFailure(RuntimeError):
    """Raised when a blocking data-quality rule fails."""


def _check(name: str, passed: bool, severity: str, measured: Any, threshold: Any, detail: str = "") -> dict[str, Any]:
    return {
        "check_name": name,
        "severity": severity,
        "status": "PASS" if passed else "FAIL",
        "measured": measured,
        "threshold": threshold,
        "detail": detail,
        "checked_at": iso_now(),
    }


def _blank_key_rows(df: pd.DataFrame, keys: list[str]) -> int:
    if df.empty:
        return 0
    missing_cols = [k for k in keys if k not in df.columns]
    if missing_cols:
        return len(df)
    return int(df[keys].isna().any(axis=1).sum() + df[keys].astype(str).apply(lambda c: c.str.strip().eq("")).any(axis=1).sum())


def _join_match_rate(left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> tuple[float, int, int]:
    if left.empty:
        return 1.0, 0, 0
    if right.empty or any(k not in left.columns for k in keys) or any(k not in right.columns for k in keys):
        return 0.0, 0, len(left)
    right_keys = right[keys].drop_duplicates().copy()
    probe = left[keys].merge(right_keys, how="left", on=keys, indicator=True)
    matched = int((probe["_merge"] == "both").sum())
    total = len(probe)
    return matched / total if total else 1.0, matched, total


def validate_data(silver: dict[str, pd.DataFrame], run_id: str, observability_dir: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    # The dictionary is an auxiliary reference table (not one of the six business entities).
    dictionary = silver.get("dicionario")
    if dictionary is None:
        results.append(_check("dictionary_reference_table_present:dicionario", False, "INFO", 0, "> 0 when official data are used", "Synthetic unit-test inputs may omit the dictionary; the real-data preparation and AWS job require it."))
    else:
        required_dict_columns = {"id_tabela", "nome_coluna", "chave", "valor"}
        has_columns = required_dict_columns.issubset(dictionary.columns)
        results.append(_check("dictionary_reference_table_schema:dicionario", has_columns, "BLOCKING", sorted(set(dictionary.columns)), sorted(required_dict_columns), "O dicionário oficial precisa conter as chaves e os rótulos."))
        results.append(_check("dictionary_reference_table_non_empty:dicionario", not dictionary.empty, "BLOCKING", len(dictionary), "> 0", "A tabela de referência deve estar disponível."))
        if has_columns and not dictionary.empty:
            duplicate_dictionary_keys = int(dictionary.duplicated(["id_tabela", "nome_coluna", "chave"]).sum())
            results.append(_check("dictionary_reference_keys_unique:dicionario", duplicate_dictionary_keys == 0, "BLOCKING", duplicate_dictionary_keys, 0, "Cada código por tabela/coluna deve ter no máximo um rótulo."))
            for table_name in ["alunos", "municipio", "uf", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio"]:
                frame = silver.get(table_name, pd.DataFrame())
                if frame.empty:
                    continue
                for category in ["rede", "serie", "presenca", "alfabetizado", "preenchimento_caderno"]:
                    description_col = f"{category}_descricao"
                    if category not in frame.columns or description_col not in frame.columns:
                        continue
                    values = frame[category].dropna()
                    # Coded numeric domains must have a dictionary entry. Text-labelled synthetic
                    # fixtures are informative only, since they are not official coded exports.
                    numeric_values = pd.to_numeric(values.astype("string"), errors="coerce")
                    if len(values) and numeric_values.notna().all():
                        unmapped = int((frame[category].notna() & frame[description_col].isna()).sum())
                        results.append(_check(
                            f"dictionary_code_coverage:{table_name}.{category}", unmapped == 0, "BLOCKING",
                            unmapped, 0, "Todo código numérico presente na tabela precisa ter rótulo correspondente no dicionário oficial."
                        ))

    # All six required entities must actually contain data in this run.
    for table in TABLES:
        df = silver.get(table, pd.DataFrame())
        results.append(_check(
            f"required_table_non_empty:{table}", not df.empty, "BLOCKING", len(df), "> 0",
            "A entidade é obrigatória para a pipeline; não basta ingeri-la e descartá-la.",
        ))

    key_map = {
        "uf": ["ano", "sigla_uf", "serie", "rede"],
        "municipio": ["ano", "id_municipio", "serie", "rede"],
        "meta_alfabetizacao_brasil": ["ano", "serie", "rede"],
        "meta_alfabetizacao_uf": ["ano", "sigla_uf", "serie", "rede"],
        "meta_alfabetizacao_municipio": ["ano", "id_municipio", "serie", "rede"],
        "alunos": ["id_aluno", "ano", "id_municipio", "serie", "rede"],
    }
    for table, keys in key_map.items():
        df = silver.get(table, pd.DataFrame())
        missing = _blank_key_rows(df, keys)
        results.append(_check(f"missing_keys:{table}", missing == 0, "BLOCKING", missing, 0, f"Chaves avaliadas: {keys}"))
        if not df.empty and set(keys).issubset(df.columns):
            duplicates = int(df.duplicated(keys).sum())
            # Microdata should also be unique by id_aluno; aggregate tables are unique by natural key.
            results.append(_check(f"duplicate_natural_keys:{table}", duplicates == 0, "BLOCKING", duplicates, 0, f"Chaves: {keys}"))

    for table, df in [(name, silver.get(name, pd.DataFrame())) for name in TABLES]:
        keys = key_map.get(table, [])
        missing_counts = {col: int(df[col].isna().sum()) for col in df.columns if col not in keys and int(df[col].isna().sum()) > 0}
        results.append(_check(f"missing_values:{table}", True, "INFO", missing_counts, "registrar ausências", "Ausências são reportadas; as variáveis críticas têm regras bloqueantes específicas."))

    for table in ["uf", "municipio"]:
        df = silver.get(table, pd.DataFrame())
        if not df.empty and "taxa_alfabetizacao" in df.columns:
            valid = df["taxa_alfabetizacao"].dropna().between(0, 100).all()
            out_of_range = int((~df["taxa_alfabetizacao"].dropna().between(0, 100)).sum())
            results.append(_check(f"percentage_range:{table}", bool(valid), "BLOCKING", out_of_range, 0, "Percentuais devem estar entre 0 e 100."))

    students = silver.get("alunos", pd.DataFrame())
    if not students.empty:
        missing_presence = int(students["presenca"].isna().sum())
        results.append(_check("missing_presence:alunos", missing_presence == 0, "BLOCKING", missing_presence, 0, "Presença deve estar traduzida pelo dicionário oficial; não inferir código desconhecido."))
        present = students[students["presenca"] == 1]
        missing_literacy = int(present["alfabetizado"].isna().sum())
        results.append(_check("missing_literacy_status:present_students", missing_literacy == 0, "BLOCKING", missing_literacy, 0, "O resultado de alfabetização deve estar disponível para os alunos presentes."))
        invalid_weights = int((present["peso_amostral"].isna() | (present["peso_amostral"] <= 0)).sum())
        invalid_weight_rate = invalid_weights / len(present) if len(present) else 0.0
        results.append(_check(
            "valid_sample_weight_rate:present_students", invalid_weight_rate <= MAX_INVALID_WEIGHT_RATE,
            "BLOCKING", round(invalid_weight_rate, 6), f"<= {MAX_INVALID_WEIGHT_RATE:.4f}",
            f"{invalid_weights}/{len(present)} pesos ausentes/não positivos; linhas inelegíveis são excluídas do cálculo ponderado, nunca preenchidas com zero."
        ))
        results.append(_check("invalid_sample_weight_count:present_students", True, "INFO", invalid_weights, "registrar e excluir do cálculo", "A ausência de peso não é convertida em zero."))
        results.append(_check("missing_student_fields:all_rows", True, "INFO", {col: int(students[col].isna().sum()) for col in ["presenca", "alfabetizado", "peso_amostral", "proficiencia"] if col in students.columns}, "registrar nulos", "Nulos em variáveis analíticas são registrados; campos críticos têm regras bloqueantes específicas."))

    for table in ["meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio"]:
        df = silver.get(table, pd.DataFrame())
        if not df.empty and "meta_alfabetizacao" in df.columns:
            invalid = int((~df["meta_alfabetizacao"].dropna().between(0, 100)).sum())
            results.append(_check(f"target_range:{table}", invalid == 0, "BLOCKING", invalid, 0, "Metas devem estar entre 0 e 100."))

    # Measure real join coverage after each critical integration.
    students = silver.get("alunos", pd.DataFrame())
    municipality = silver.get("municipio", pd.DataFrame())
    if not students.empty and not municipality.empty:
        student_groups = students[["ano", "id_municipio", "serie", "rede"]].drop_duplicates()
        comparable_networks = set(students["rede"].dropna().astype(str)) & set(municipality["rede"].dropna().astype(str))
        not_comparable = int((~student_groups["rede"].astype(str).isin(comparable_networks)).sum())
        results.append(_check(
            "student_network_groups_not_comparable_to_aggregates", True, "INFO", not_comparable,
            "documentar categorias sem par", f"Categorias de rede sem correspondência na tabela agregada: {sorted(set(students['rede'].dropna().astype(str)) - comparable_networks)}."
        ))
        student_groups = student_groups[student_groups["rede"].astype(str).isin(comparable_networks)]
        rate, matched, total = _join_match_rate(student_groups, municipality, ["ano", "id_municipio", "serie", "rede"])
        results.append(_check("join_match:alunos_to_municipio", rate >= MIN_JOIN_MATCH_RATE, "BLOCKING", round(rate, 4), f">= {MIN_JOIN_MATCH_RATE:.2f}", f"{matched}/{total} grupos comparáveis de alunos encontrados no indicador municipal."))

    uf = silver.get("uf", pd.DataFrame())
    if not municipality.empty and not uf.empty:
        municipality_keys = municipality[["ano", "sigla_uf", "serie", "rede"]].drop_duplicates()
        rate, matched, total = _join_match_rate(municipality_keys, uf, ["ano", "sigla_uf", "serie", "rede"])
        results.append(_check("join_match:municipio_to_uf", rate >= MIN_JOIN_MATCH_RATE, "BLOCKING", round(rate, 4), f">= {MIN_JOIN_MATCH_RATE:.2f}", f"{matched}/{total} recortes municipais encontrados no agregado estadual."))

    # Goals start in 2024. Coverage is measured only for the years covered by the target table,
    # so absence of a 2023 target is not incorrectly treated as a failed join.
    for target_name, keys in [
        ("meta_alfabetizacao_municipio", ["ano", "id_municipio", "serie", "rede"]),
        ("meta_alfabetizacao_uf", ["ano", "sigla_uf", "serie", "rede"]),
        ("meta_alfabetizacao_brasil", ["ano", "serie", "rede"]),
    ]:
        targets = silver.get(target_name, pd.DataFrame())
        if targets.empty or municipality.empty:
            continue
        eligible = municipality[municipality["ano"].isin(targets["ano"].dropna().unique())]
        if "rede" in keys and "rede" in eligible.columns and "rede" in targets.columns:
            eligible = eligible[eligible["rede"].astype(str).isin(targets["rede"].dropna().astype(str).unique())]
        left = eligible[keys].drop_duplicates() if set(keys).issubset(eligible.columns) else pd.DataFrame()
        rate, matched, total = _join_match_rate(left, targets, keys) if not left.empty else (1.0, 0, 0)
        results.append(_check(f"join_match:municipio_to_{target_name}", rate >= MIN_JOIN_MATCH_RATE, "BLOCKING", round(rate, 4), f">= {MIN_JOIN_MATCH_RATE:.2f}", f"{matched}/{total} recortes elegíveis encontraram meta; anos elegíveis: {sorted(map(int, targets['ano'].dropna().unique()))}."))

    events = silver.get("streaming_events", pd.DataFrame())
    quarantine = silver.get("streaming_quarantine", pd.DataFrame())
    results.append(_check("streaming_invalid_events_quarantined", True, "INFO", len(quarantine), "registrar e isolar", "Eventos inválidos são desviados para quarantine e não entram na Gold."))
    results.append(_check("streaming_deduplicated", True, "INFO", int(silver.get("streaming_duplicate_count", 0)), "deduplicar por event_id", "Eventos repetidos são removidos antes da camada analítica."))
    results.append(_check("streaming_events_reached_gold_candidate", True, "INFO", len(events), "valid events forwarded", "Eventos válidos seguem para a Gold com origem_dado=simulado."))

    observability_dir.mkdir(parents=True, exist_ok=True)
    report_path = observability_dir / f"quality_{run_id}.json"
    report = {"run_id": run_id, "created_at": iso_now(), "blocking_failures": sum(r["severity"] == "BLOCKING" and r["status"] == "FAIL" for r in results), "checks": results}
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    jsonl = observability_dir / "quality_history.jsonl"
    for result in results:
        append_jsonl(jsonl, {"run_id": run_id, **result})
    return results


def enforce_gate(results: list[dict[str, Any]]) -> None:
    failures = [r for r in results if r["severity"] == "BLOCKING" and r["status"] == "FAIL"]
    if failures:
        message = "; ".join(f"{item['check_name']}={item['measured']} (limite {item['threshold']})" for item in failures)
        raise QualityGateFailure(f"Quality gate bloqueou a publicação das camadas seguintes: {message}")
