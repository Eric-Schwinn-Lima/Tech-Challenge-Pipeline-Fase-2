"""AWS Glue 4.0 Spark job. Inputs are canonical CSVs in S3; outputs are run-versioned Parquet."""
import sys
import uuid
import json
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PREFIX", "STREAM_PREFIX", "OUTPUT_PREFIX", "GOLD_PREFIX", "MIN_JOIN_MATCH_RATE", "MAX_INVALID_WEIGHT_RATE"])
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
s3 = boto3.client("s3")
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
pipeline_started_at = datetime.now(timezone.utc)
source_prefix = args["SOURCE_PREFIX"].rstrip("/")
stream_prefix = args["STREAM_PREFIX"].rstrip("/")
output_prefix = args["OUTPUT_PREFIX"].rstrip("/")
gold_prefix = args["GOLD_PREFIX"].rstrip("/")
min_match = float(args["MIN_JOIN_MATCH_RATE"])
max_invalid_weight_rate = float(args["MAX_INVALID_WEIGHT_RATE"])
lake_bucket = source_prefix.split("/")[2]
quality_checks = []
frames = {}
gold_outputs = {}

def write_run_metrics(status, error=None):
    finished = datetime.now(timezone.utc)
    row_counts = {name: frame.count() for name, frame in frames.items()}
    gold_counts = {name: frame.count() for name, frame in gold_outputs.items()}
    metric_body = {
        "run_id": run_id,
        "status": status,
        "started_at": pipeline_started_at.isoformat().replace("+00:00", "Z"),
        "finished_at": finished.isoformat().replace("+00:00", "Z"),
        "duration_seconds": (finished - pipeline_started_at).total_seconds(),
        "source_rows": row_counts,
        "gold_rows": gold_counts,
        "join_rate_students_municipio": globals().get("match_rate"),
        "join_rate_municipio_uf": globals().get("uf_rate"),
        "error": error,
    }
    s3.put_object(Bucket=lake_bucket, Key=f"observability/metrics/run_id={run_id}.json", Body=json.dumps(metric_body, ensure_ascii=False, indent=2).encode("utf-8"), ContentType="application/json")

def write_quality_report(status, error=None):
    body = {
        "run_id": run_id,
        "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": status,
        "error": error,
        "checks": quality_checks,
    }
    s3.put_object(Bucket=lake_bucket, Key=f"observability/quality/run_id={run_id}.json", Body=json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8"), ContentType="application/json")

def record_check(name, passed, measured, threshold, detail=""):
    quality_checks.append({"check_name": name, "severity": "BLOCKING", "status": "PASS" if passed else "FAIL", "measured": measured, "threshold": threshold, "detail": detail})
    if not passed:
        fail(f"{name}: measured={measured}; threshold={threshold}; {detail}")

tables = ["uf", "municipio", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "alunos"]
required = {
    "uf": ["ano", "sigla_uf", "serie", "rede", "taxa_alfabetizacao"],
    "municipio": ["ano", "id_municipio", "sigla_uf", "serie", "rede", "taxa_alfabetizacao"],
    "meta_alfabetizacao_brasil": ["ano", "serie", "rede", "meta_alfabetizacao"],
    "meta_alfabetizacao_uf": ["ano", "sigla_uf", "serie", "rede", "meta_alfabetizacao"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio", "serie", "rede", "meta_alfabetizacao"],
    "alunos": ["id_aluno", "ano", "id_municipio", "sigla_uf", "serie", "rede", "presenca", "alfabetizado", "peso_amostral"],
}

def fail(message):
    quality_checks.append({"check_name": "quality_gate_exception", "severity": "BLOCKING", "status": "FAIL", "measured": message, "threshold": "all blocking rules pass", "detail": "A camada seguinte não deve ser publicada."})
    try:
        write_quality_report("FAILED", error=message)
        write_run_metrics("FAILED", error=message)
    except Exception as report_error:
        print(f"Could not persist quality/metrics report to S3: {report_error}")
    raise RuntimeError("QUALITY_GATE_BLOCKED: " + message)

def require_columns(name, df):
    missing = [c for c in required[name] if c not in df.columns]
    if missing:
        fail(f"Tabela {name} sem colunas obrigatórias: {missing}")
    row_count = df.count()
    record_check(f"required_table_non_empty:{name}", row_count > 0, row_count, "> 0", "Entidade obrigatória precisa ser usada na pipeline.")

def normalize(df):
    if "id_municipio" in df.columns:
        df = df.withColumn("id_municipio", F.lpad(F.regexp_replace(F.col("id_municipio").cast("string"), r"\.0$", ""), 7, "0"))
    if "sigla_uf" in df.columns:
        df = df.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf").cast("string"))))
    for col in ["ano"]:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("int"))
    for col in ["taxa_alfabetizacao", "meta_alfabetizacao", "peso_amostral", "proficiencia"]:
        if col in df.columns:
            df = df.withColumn(col, F.regexp_replace(F.col(col).cast("string"), ",", ".").cast("double"))
    for col in ["serie", "rede"]:
        if col in df.columns:
            df = df.withColumn(col, F.lower(F.trim(F.col(col).cast("string"))))
    for col in ["presenca", "alfabetizado"]:
        if col in df.columns:
            x = F.lower(F.trim(F.col(col).cast("string")))
            df = df.withColumn(col, F.when(x.isin("1", "true", "sim", "s", "presente", "yes"), F.lit(1.0)).when(x.isin("0", "false", "nao", "não", "n", "ausente", "no"), F.lit(0.0)).otherwise(x.cast("double")))
    return df

for name in tables:
    path = f"{source_prefix}/{name}.csv"
    frame = spark.read.option("header", True).option("inferSchema", False).csv(path)
    frame = normalize(frame)
    require_columns(name, frame)
    frames[name] = frame

# Load the official reference dictionary. It is auxiliary to the six required business
# entities but is used in the transformation, and is retained in Silver for lineage.
dictionary_path = f"{source_prefix}/dicionario.csv"
dictionary = spark.read.option("header", True).option("inferSchema", False).csv(dictionary_path)
required_dictionary_columns = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]
missing_dictionary_columns = [c for c in required_dictionary_columns if c not in dictionary.columns]
if missing_dictionary_columns:
    fail(f"Tabela dicionario sem colunas obrigatórias: {missing_dictionary_columns}")
dictionary = dictionary.select(*required_dictionary_columns)
dictionary = dictionary.withColumn("id_tabela", F.lower(F.trim(F.col("id_tabela")))) \
    .withColumn("nome_coluna", F.lower(F.trim(F.col("nome_coluna")))) \
    .withColumn("chave", F.trim(F.col("chave"))) \
    .withColumn("valor", F.trim(F.col("valor")))
dictionary_rows = dictionary.count()
record_check("required_reference_table_non_empty:dicionario", dictionary_rows > 0, dictionary_rows, "> 0", "O dicionário oficial precisa estar disponível para traduzir códigos de categoria.")
frames["dicionario"] = dictionary

# Add official descriptions but never replace coded keys used by joins. Some target tables
# use short textual network labels; their code mapping is handled by prepare_real_data.py
# from the same dictionary, with the semantic alias documented in README.
def _dictionary_key(column):
    return F.regexp_replace(F.trim(F.col(column).cast("string")), r"\.0$", "")

entity_dictionary = {
    "alunos": "alunos",
    "municipio": "municipio",
    "uf": "uf",
    "meta_alfabetizacao_municipio": "municipio",
    "meta_alfabetizacao_uf": "uf",
    "meta_alfabetizacao_brasil": "uf",
}
for table_name, dictionary_entity in entity_dictionary.items():
    frame = frames[table_name]
    for category_column in ["rede", "serie", "presenca", "alfabetizado", "preenchimento_caderno"]:
        if category_column not in frame.columns:
            continue
        lookup = dictionary.filter(
            (F.col("id_tabela") == dictionary_entity) & (F.col("nome_coluna") == category_column)
        ).select(
            F.regexp_replace(F.trim(F.col("chave")), r"\.0$", "").alias("__dictionary_key"),
            F.col("valor").alias(f"{category_column}_descricao"),
        ).dropDuplicates(["__dictionary_key"])
        if lookup.limit(1).count() == 0:
            continue
        temp_key = f"__{category_column}_dictionary_key"
        frame = frame.withColumn(temp_key, _dictionary_key(category_column))
        frame = frame.join(lookup, frame[temp_key] == lookup["__dictionary_key"], "left").drop(temp_key, "__dictionary_key")
    frames[table_name] = frame

# Blocking structural and integrity checks before publishing the layer.
keysets = {
    "uf": ["ano", "sigla_uf", "serie", "rede"],
    "municipio": ["ano", "id_municipio", "serie", "rede"],
    "meta_alfabetizacao_brasil": ["ano", "serie", "rede"],
    "meta_alfabetizacao_uf": ["ano", "sigla_uf", "serie", "rede"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio", "serie", "rede"],
    "alunos": ["id_aluno", "ano", "id_municipio", "serie", "rede"],
}
for name, keys in keysets.items():
    df = frames[name]
    missing = df.filter(F.expr(" OR ".join([f"`{k}` IS NULL OR trim(cast(`{k}` as string)) = ''" for k in keys]))).count()
    record_check(f"missing_keys:{name}", missing == 0, missing, 0, f"Chaves avaliadas: {keys}")
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
    record_check(f"duplicate_natural_keys:{name}", duplicates == 0, duplicates, 0, f"Chaves: {keys}")
for name, df in frames.items():
    missing_counts = {}
    for column in df.columns:
        missing_count = df.filter(F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")).count()
        if missing_count:
            missing_counts[column] = missing_count
    quality_checks.append({"check_name": f"missing_values:{name}", "severity": "INFO", "status": "INFO", "measured": missing_counts, "threshold": "registrar ausências", "detail": "As variáveis críticas têm verificações bloqueantes separadas."})

for name in ["uf", "municipio"]:
    bad = frames[name].filter(F.col("taxa_alfabetizacao").isNotNull() & ~F.col("taxa_alfabetizacao").between(0, 100)).count()
    record_check(f"percentage_range:{name}", bad == 0, bad, 0, "Percentuais entre 0 e 100.")
for name in ["meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio"]:
    bad = frames[name].filter(F.col("meta_alfabetizacao").isNotNull() & ~F.col("meta_alfabetizacao").between(0, 100)).count()
    record_check(f"target_range:{name}", bad == 0, bad, 0, "Metas entre 0 e 100.")

student_rows = frames["alunos"]
missing_presence = student_rows.filter(F.col("presenca").isNull()).count()
record_check("missing_presence:alunos", missing_presence == 0, missing_presence, 0, "Presença traduzida pelo dicionário oficial.")
present_rows = student_rows.filter(F.col("presenca") == 1)
missing_status = present_rows.filter(F.col("alfabetizado").isNull()).count()
record_check("missing_literacy_status:present_students", missing_status == 0, missing_status, 0, "Resultado obrigatório para aluno presente/avaliado.")
invalid_weights = present_rows.filter(F.col("peso_amostral").isNull() | (F.col("peso_amostral") <= 0)).count()
present_count = present_rows.count()
invalid_weight_rate = invalid_weights / present_count if present_count else 0.0
record_check(
    "valid_sample_weight_rate:present_students", invalid_weight_rate <= max_invalid_weight_rate,
    round(invalid_weight_rate, 6), f"<= {max_invalid_weight_rate:.4f}",
    f"{invalid_weights}/{present_count} pesos ausentes/não positivos; linhas inelegíveis são excluídas do indicador.",
)
quality_checks.append({"check_name": "invalid_sample_weight_count:present_students", "severity": "INFO", "status": "INFO", "measured": invalid_weights, "threshold": "registrar e excluir do cálculo", "detail": "Peso ausente não é convertido em zero."})

municipio = frames["municipio"]
uf = frames["uf"]
alunos = frames["alunos"]
student_groups = alunos.select("ano", "id_municipio", "serie", "rede").distinct()
comparable_networks = {r[0] for r in alunos.select("rede").distinct().collect()} & {r[0] for r in municipio.select("rede").distinct().collect()}
not_comparable = student_groups.filter(~F.col("rede").isin(sorted(comparable_networks))).count() if comparable_networks else student_groups.count()
quality_checks.append({"check_name": "student_network_groups_not_comparable_to_aggregates", "severity": "INFO", "status": "INFO", "measured": not_comparable, "threshold": "documentar categorias sem par", "detail": f"Categorias sem correspondência na tabela agregada: {sorted({r[0] for r in alunos.select('rede').distinct().collect()} - comparable_networks)}"})
student_groups = student_groups.filter(F.col("rede").isin(sorted(comparable_networks)))
joined = student_groups.join(municipio.select("ano", "id_municipio", "serie", "rede").distinct(), ["ano", "id_municipio", "serie", "rede"], "left_semi").count()
total = student_groups.count()
match_rate = joined / total if total else 1.0
record_check("join_match:alunos_to_municipio", match_rate >= min_match, round(match_rate, 4), f">= {min_match:.4f}", f"{joined}/{total} grupos comparáveis de alunos encontrados no indicador municipal.")

municipio_groups = municipio.select("ano", "sigla_uf", "serie", "rede").distinct()
matched_uf = municipio_groups.join(uf.select("ano", "sigla_uf", "serie", "rede").distinct(), ["ano", "sigla_uf", "serie", "rede"], "left_semi").count()
total_uf = municipio_groups.count()
uf_rate = matched_uf / total_uf if total_uf else 1.0
record_check("join_match:municipio_to_uf", uf_rate >= min_match, round(uf_rate, 4), f">= {min_match:.4f}", f"{matched_uf}/{total_uf} recortes encontrados no agregado estadual.")

for target_name, keys in [
    ("meta_alfabetizacao_municipio", ["ano", "id_municipio", "serie", "rede"]),
    ("meta_alfabetizacao_uf", ["ano", "sigla_uf", "serie", "rede"]),
    ("meta_alfabetizacao_brasil", ["ano", "serie", "rede"]),
]:
    target = frames[target_name]
    covered_years = [r[0] for r in target.select("ano").distinct().collect()]
    eligible = municipio.filter(F.col("ano").isin(covered_years))
    if "rede" in keys:
        target_networks = [r[0] for r in target.select("rede").distinct().collect()]
        eligible = eligible.filter(F.col("rede").isin(target_networks))
    eligible = eligible.select(*keys).distinct()
    target_keys = target.select(*keys).distinct()
    eligible_total = eligible.count()
    eligible_matched = eligible.join(target_keys, keys, "left_semi").count()
    coverage = eligible_matched / eligible_total if eligible_total else 1.0
    record_check(f"join_match:municipio_to_{target_name}", coverage >= min_match, round(coverage, 4), f">= {min_match:.4f}", f"{eligible_matched}/{eligible_total} recortes elegíveis; anos cobertos pela meta: {sorted(covered_years)}")

write_quality_report("PASSED")

# Student microdata: include only evaluated/present students with a valid sample weight.
assessed = alunos.filter((F.col("presenca") == 1) & F.col("alfabetizado").isNotNull() & (F.col("peso_amostral") > 0))
calculated = assessed.groupBy("ano", "id_municipio", "sigla_uf", "serie", "rede").agg(
    (F.sum(F.col("alfabetizado") * F.col("peso_amostral")) / F.sum("peso_amostral") * 100).alias("taxa_calculada_microdados"),
    F.sum("peso_amostral").alias("peso_avaliado"), F.count("id_aluno").alias("alunos_avaliados"),
)

# Build Gold by joining all three target tables into the municipal fact.
meta_mun = frames["meta_alfabetizacao_municipio"].select("ano", "id_municipio", "serie", "rede", F.col("meta_alfabetizacao").alias("meta_municipio"))
meta_uf = frames["meta_alfabetizacao_uf"].select("ano", "sigla_uf", "serie", "rede", F.col("meta_alfabetizacao").alias("meta_uf"))
meta_br = frames["meta_alfabetizacao_brasil"].select("ano", "serie", "rede", F.col("meta_alfabetizacao").alias("meta_brasil"))
fact = municipio.join(meta_mun, ["ano", "id_municipio", "serie", "rede"], "left").join(meta_uf, ["ano", "sigla_uf", "serie", "rede"], "left").join(meta_br, ["ano", "serie", "rede"], "left")
fact = fact.withColumn("gap_meta_municipio_pp", F.col("taxa_alfabetizacao") - F.col("meta_municipio"))
municipal_target_networks = [r[0] for r in meta_mun.select("rede").distinct().collect()]
fact = fact.withColumn(
    "status_meta_municipio",
    F.when(~F.col("rede").isin(municipal_target_networks), "NAO_COMPARAVEL")
     .when(F.col("taxa_alfabetizacao").isNull(), "SEM_RESULTADO")
     .when(F.col("meta_municipio").isNull(), "SEM_META")
     .when(F.col("taxa_alfabetizacao") >= F.col("meta_municipio"), "ATINGIDA")
     .otherwise("ABAIXO_DA_META"),
)
fact = fact.withColumn("origem_dado", F.lit("oficial_tabela_agregada"))

uf_mart = uf.join(meta_uf, ["ano", "sigla_uf", "serie", "rede"], "left").join(meta_br, ["ano", "serie", "rede"], "left")
uf_mart = uf_mart.withColumn("gap_meta_uf_pp", F.col("taxa_alfabetizacao") - F.col("meta_uf"))
uf_target_networks = [r[0] for r in meta_uf.select("rede").distinct().collect()]
uf_mart = uf_mart.withColumn(
    "status_meta_uf",
    F.when(~F.col("rede").isin(uf_target_networks), "NAO_COMPARAVEL")
     .when(F.col("taxa_alfabetizacao").isNull(), "SEM_RESULTADO")
     .when(F.col("meta_uf").isNull(), "SEM_META")
     .when(F.col("taxa_alfabetizacao") >= F.col("meta_uf"), "ATINGIDA")
     .otherwise("ABAIXO_DA_META"),
)
uf_mart = uf_mart.withColumn("origem_dado", F.lit("oficial_tabela_agregada"))

comparable_rede_values = [r[0] for r in municipio.select("rede").distinct().collect()]
calculated_comparable = calculated.filter(F.col("rede").isin(comparable_rede_values))
reconciliation = calculated_comparable.join(municipio.select("ano", "id_municipio", "sigla_uf", "serie", "rede", F.col("taxa_alfabetizacao").alias("taxa_oficial_publicada")), ["ano", "id_municipio", "sigla_uf", "serie", "rede"], "left")
reconciliation = reconciliation.withColumn("diferenca_pontos_percentuais", F.col("taxa_calculada_microdados") - F.col("taxa_oficial_publicada"))
evolution = municipio.groupBy("ano", "serie", "rede").agg(F.avg("taxa_alfabetizacao").alias("media_simples_municipal_percentual"), F.countDistinct("id_municipio").alias("municipios_com_resultado"))
evolution = evolution.withColumn("indicador_agregado", F.lit("media_simples_municipal_nao_oficial"))

# The stream path can be empty on the first deployment.
# Invalid or incompatible events are quarantined, without blocking Batch.

stream = None

bucket_name = source_prefix.split("/")[2]
stream_prefix_key = stream_prefix.replace(
    f"s3://{bucket_name}/", ""
)

resp = s3.list_objects_v2(
    Bucket=bucket_name,
    Prefix=stream_prefix_key,
    MaxKeys=1000,
)

stream_uris = [
    f"s3://{bucket_name}/{obj['Key']}"
    for obj in resp.get("Contents", [])
    if obj["Key"].endswith((".json", ".jsonl"))
]

if stream_uris:
    raw_stream = spark.read.json(stream_uris)

    if "event_id" in raw_stream.columns:
        raw_stream = raw_stream.dropDuplicates(["event_id"])

    required_stream_columns = [
        "event_id",
        "event_timestamp",
        "ano",
        "id_municipio",
        "sigla_uf",
        "serie",
        "rede",
        "taxa_alfabetizacao",
    ]

    missing_stream_columns = [
        column
        for column in required_stream_columns
        if column not in raw_stream.columns
    ]

    quarantine_path = (
        f"s3://{lake_bucket}/observability/"
        f"quarantine/run_id={run_id}/streaming_invalid"
    )

    # If the entire input schema lacks required columns,
    # quarantine the records without referencing missing columns.
    if missing_stream_columns:
        invalid = raw_stream.withColumn(
            "validation_error",
            F.lit(
                "missing_required_columns:"
                + ",".join(missing_stream_columns)
            ),
        )

        if invalid.limit(1).count() > 0:
            invalid.write.mode("append").json(quarantine_path)

        print(
            "STREAM_SCHEMA_INVALID: "
            f"missing_columns={missing_stream_columns}. "
            "Events quarantined; Batch processing will continue."
        )

        stream = None

    else:
        raw_stream = raw_stream.withColumn(
            "taxa_alfabetizacao",
            F.col("taxa_alfabetizacao").cast("double"),
        )

        invalid_condition = F.lit(False)

        for event_col in required_stream_columns:
            invalid_condition = (
                invalid_condition
                | F.col(event_col).isNull()
                | (
                    F.trim(F.col(event_col).cast("string")) == ""
                )
            )

        invalid_condition = (
            invalid_condition
            | F.col("taxa_alfabetizacao").isNull()
            | ~F.col("taxa_alfabetizacao").between(0, 100)
        )

        invalid = raw_stream.filter(invalid_condition).withColumn(
            "validation_error",
            F.lit("missing_or_invalid_required_field"),
        )

        if invalid.limit(1).count() > 0:
            invalid.write.mode("append").json(quarantine_path)

        valid_stream = raw_stream.filter(~invalid_condition)

        # Publish streaming events only when valid records exist.
        if valid_stream.limit(1).count() > 0:
            stream = (
                valid_stream
                .withColumn(
                    "origem_dado",
                    F.coalesce(
                        F.col("origem_dado"),
                        F.lit("simulado"),
                    ),
                )
                .withColumn(
                    "status_evento",
                    F.lit("SIMULADO_NAO_OFICIAL"),
                )
            )
        else:
            stream = None

silver_outputs = dict(frames)
silver_outputs["indicador_calculado_microdados"] = calculated
gold_outputs = {
    "fato_alfabetizacao_municipio": fact,
    "mart_alfabetizacao_uf": uf_mart,
    "mart_evolucao_temporal": evolution,
    "reconciliacao_microdados": reconciliation,
}
if stream is not None:
    gold_outputs["mart_streaming_eventos"] = stream
for name, frame in gold_outputs.items():
    if name != "mart_streaming_eventos" and frame.limit(1).count() == 0:
        fail(f"Gold table empty before publish: {name}")
for name, frame in silver_outputs.items():
    frame.write.mode("errorifexists").option("compression", "snappy").parquet(f"{output_prefix}/{name}/run_id={run_id}")

for name, frame in gold_outputs.items():
    frame.write.mode("errorifexists").option("compression", "snappy").parquet(f"{gold_prefix}/{name}/run_id={run_id}")

write_run_metrics("SUCCESS")
print({"run_id": run_id, "rows_source": {k: v.count() for k, v in frames.items()}, "join_rate_students_municipio": match_rate, "join_rate_municipio_uf": uf_rate, "gold_tables": list(gold_outputs.keys())})
