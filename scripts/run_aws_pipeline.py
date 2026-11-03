#!/usr/bin/env python3
"""Run the deployed Step Functions pipeline, refresh Glue crawlers and validate Gold in Athena.

This operational wrapper deliberately does not modify Terraform or AWS resource definitions.
It reuses the existing state machine, Glue job, Glue crawlers, Data Catalog and Athena workgroup.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

LOG = logging.getLogger("run_aws_pipeline")
CORE_GOLD_TABLES = {
    "fato_alfabetizacao_municipio",
    "mart_alfabetizacao_uf",
    "mart_evolucao_temporal",
    "reconciliacao_microdados",
    "mart_perfil_proficiencia",
    "mart_acompanhamento_metas",
    "mart_trajetoria_metas",
}
TABLE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
S3_URI_RE = re.compile(r"^s3://([^/]+)/(.+?)/?$")


class PipelineError(RuntimeError):
    """Raised when an AWS pipeline stage fails or its result is inconsistent."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute Step Functions, refresh Glue Catalog and validate Gold in Athena."
    )
    parser.add_argument("--profile", default="AWS-tech-challenge", help="AWS CLI/SDK profile")
    parser.add_argument("--region", default="us-east-2", help="AWS region")
    parser.add_argument(
        "--state-machine-arn",
        default=None,
        help="Optional explicit Step Functions ARN. If omitted, discover the unique *pipeline-orchestrator machine.",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Optional Glue database. If omitted, infer it from the matching Silver/Gold crawlers.",
    )
    parser.add_argument(
        "--timeout-minutes",
        type=int,
        default=120,
        help="Timeout for Step Functions, crawlers and Athena (default: 120 minutes).",
    )
    parser.add_argument(
        "--poll-seconds", type=int, default=10, help="Polling interval (default: 10 seconds)."
    )
    parser.add_argument(
        "--output-dir",
        default="data/observability",
        help="Local directory for the JSON validation summary.",
    )
    parser.add_argument(
        "--existing-run-id",
        default=None,
        help=(
            "Repair/catalog-validate an already successful Glue run without starting "
            "Step Functions or rerunning crawlers. Reads observability/metrics/run_id=<id>.json."
        ),
    )
    return parser.parse_args()


def parse_s3_uri(uri: str) -> tuple[str, str]:
    match = S3_URI_RE.match(uri.strip())
    if not match:
        raise PipelineError(f"Invalid S3 URI in Glue job configuration: {uri!r}")
    return match.group(1), match.group(2).rstrip("/")


def normalize_s3_prefix(uri: str) -> str:
    return uri.rstrip("/") + "/"


def discover_state_machine(sfn: Any, explicit_arn: str | None) -> str:
    if explicit_arn:
        return explicit_arn
    paginator = sfn.get_paginator("list_state_machines")
    matches = []
    # Older botocore service models do not expose stateMachineType for this
    # operation. Listing all machines is compatible across SDK versions; the
    # unique suffix check below selects the intended pipeline machine.
    for page in paginator.paginate():
        for item in page.get("stateMachines", []):
            if item["name"].endswith("pipeline-orchestrator"):
                matches.append(item["stateMachineArn"])
    if len(matches) != 1:
        raise PipelineError(
            "Could not uniquely discover the pipeline Step Functions machine. "
            f"Found {len(matches)}; pass --state-machine-arn explicitly."
        )
    return matches[0]


def discover_job_name(sfn: Any, state_machine_arn: str) -> str:
    machine = sfn.describe_state_machine(stateMachineArn=state_machine_arn)
    definition = json.loads(machine["definition"])
    candidates = []
    for state in definition.get("States", {}).values():
        resource = state.get("Resource", "")
        if state.get("Type") == "Task" and "glue:startJobRun" in resource:
            job_name = state.get("Parameters", {}).get("JobName")
            if job_name:
                candidates.append(job_name)
    if len(candidates) != 1:
        raise PipelineError(
            "Expected exactly one Glue job in the state machine definition; "
            f"found {len(candidates)}. Inspect the state machine before proceeding."
        )
    return candidates[0]


def list_crawlers(glue: Any) -> list[dict[str, Any]]:
    paginator = glue.get_paginator("get_crawlers")
    result: list[dict[str, Any]] = []
    for page in paginator.paginate():
        result.extend(page.get("Crawlers", []))
    # GetCrawler is the authoritative detail call for Targets if a list response
    # omits that optional structure in a particular SDK/API response.
    detailed: list[dict[str, Any]] = []
    for crawler in result:
        if "Targets" not in crawler:
            crawler = glue.get_crawler(Name=crawler["Name"])["Crawler"]
        detailed.append(crawler)
    return detailed


def crawler_has_target(crawler: dict[str, Any], prefix: str) -> bool:
    expected = normalize_s3_prefix(prefix)
    targets = crawler.get("Targets", {}).get("S3Targets", [])
    return any(normalize_s3_prefix(target.get("Path", "")) == expected for target in targets)


def discover_crawlers_and_database(
    glue: Any, silver_prefix: str, gold_prefix: str, database_override: str | None
) -> tuple[str, str, str]:
    crawlers = list_crawlers(glue)
    silver = [c for c in crawlers if crawler_has_target(c, silver_prefix)]
    gold = [c for c in crawlers if crawler_has_target(c, gold_prefix)]

    if len(silver) != 1 or len(gold) != 1:
        raise PipelineError(
            "Could not uniquely identify the Silver/Gold crawlers by their S3 targets. "
            f"Silver matches={len(silver)}, Gold matches={len(gold)}."
        )

    silver_crawler = silver[0]
    gold_crawler = gold[0]
    silver_db = silver_crawler["DatabaseName"]
    gold_db = gold_crawler["DatabaseName"]
    if silver_db != gold_db:
        raise PipelineError(
            f"Silver and Gold crawlers use different databases: {silver_db!r} vs {gold_db!r}."
        )
    database = database_override or gold_db
    if database != gold_db:
        raise PipelineError(
            f"Provided database {database!r} does not match Gold crawler database {gold_db!r}."
        )
    return silver_crawler["Name"], gold_crawler["Name"], database


def discover_environment(
    sfn: Any, glue: Any, state_machine_arn: str, database_override: str | None
) -> dict[str, str]:
    job_name = discover_job_name(sfn, state_machine_arn)
    job = glue.get_job(JobName=job_name)["Job"]
    defaults = job.get("DefaultArguments", {})
    gold_uri = defaults.get("--GOLD_PREFIX")
    silver_uri = defaults.get("--OUTPUT_PREFIX")
    if not gold_uri or not silver_uri:
        raise PipelineError(
            f"Glue job {job_name!r} lacks --GOLD_PREFIX or --OUTPUT_PREFIX in DefaultArguments."
        )
    bucket, _ = parse_s3_uri(gold_uri)
    silver_bucket, _ = parse_s3_uri(silver_uri)
    if bucket != silver_bucket:
        raise PipelineError("Silver and Gold prefixes point to different S3 buckets.")

    silver_crawler, gold_crawler, database = discover_crawlers_and_database(
        glue, silver_uri, gold_uri, database_override
    )
    return {
        "job_name": job_name,
        "bucket": bucket,
        "silver_prefix": normalize_s3_prefix(silver_uri),
        "gold_prefix": normalize_s3_prefix(gold_uri),
        "silver_crawler": silver_crawler,
        "gold_crawler": gold_crawler,
        "database": database,
    }


def run_state_machine(
    sfn: Any, state_machine_arn: str, timeout_seconds: int, poll_seconds: int
) -> datetime:
    started_at = datetime.now(timezone.utc)
    execution_name = (
        "gold-validate-"
        + started_at.strftime("%Y%m%dT%H%M%S")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    response = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({"requested_by": "run_aws_pipeline.py"}),
    )
    execution_arn = response["executionArn"]
    LOG.info("Started Step Functions execution: %s", execution_arn)

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        execution = sfn.describe_execution(executionArn=execution_arn)
        status = execution["status"]
        LOG.info("Step Functions status: %s", status)
        if status == "SUCCEEDED":
            return started_at
        if status in {"FAILED", "TIMED_OUT", "ABORTED"}:
            raise PipelineError(
                "Step Functions execution failed: "
                f"status={status}; error={execution.get('error')}; cause={execution.get('cause')}"
            )
        time.sleep(poll_seconds)
    raise PipelineError(f"Step Functions execution exceeded {timeout_seconds} seconds.")


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def find_successful_run_metrics(
    s3: Any, bucket: str, execution_started_at: datetime | None,
    timeout_seconds: int, poll_seconds: int
) -> tuple[str, dict[str, Any]]:
    deadline = time.monotonic() + min(timeout_seconds, 300)
    prefix = "observability/metrics/"
    cutoff = (execution_started_at or datetime.now(timezone.utc)) - timedelta(minutes=2)

    while time.monotonic() < deadline:
        paginator = s3.get_paginator("list_objects_v2")
        candidates: list[dict[str, Any]] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json") and obj["LastModified"] >= cutoff:
                    candidates.append(obj)
        candidates.sort(key=lambda item: item["LastModified"], reverse=True)

        for obj in candidates:
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            try:
                metrics = json.loads(body)
            except json.JSONDecodeError:
                continue
            run_id = metrics.get("run_id")
            started = parse_iso_datetime(metrics.get("started_at"))
            if (
                metrics.get("status") == "SUCCESS"
                and run_id
                and isinstance(metrics.get("gold_rows"), dict)
                and started is not None
                and started >= cutoff
            ):
                LOG.info("Found successful Glue metrics for run_id=%s", run_id)
                return run_id, metrics
        time.sleep(poll_seconds)
    raise PipelineError(
        "No successful observability/metrics JSON for the current execution was found. "
        "Check the Glue job logs and S3 observability/metrics prefix."
    )


def wait_until_crawler_ready(
    glue: Any, crawler_name: str, timeout_seconds: int, poll_seconds: int
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
        state = crawler.get("State")
        if state == "READY":
            return crawler
        if state not in {"RUNNING", "STOPPING"}:
            raise PipelineError(f"Crawler {crawler_name!r} entered unexpected state {state!r}.")
        LOG.info("Crawler %s state: %s", crawler_name, state)
        time.sleep(poll_seconds)
    raise PipelineError(f"Timed out waiting for crawler {crawler_name!r} to become READY.")


def run_crawler(
    glue: Any, crawler_name: str, timeout_seconds: int, poll_seconds: int
) -> dict[str, Any]:
    # Do not assume a crawler that was already RUNNING scanned the data from this run.
    before = wait_until_crawler_ready(glue, crawler_name, timeout_seconds, poll_seconds)
    previous_start = (before.get("LastCrawl") or {}).get("StartTime")
    LOG.info("Starting Glue crawler: %s", crawler_name)
    try:
        glue.start_crawler(Name=crawler_name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "CrawlerRunningException":
            raise PipelineError(
                f"Crawler {crawler_name!r} became RUNNING concurrently. "
                "Wait for it to finish, then rerun the catalog/validation step."
            ) from exc
        raise

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
        state = crawler.get("State")
        last_crawl = crawler.get("LastCrawl") or {}
        status = last_crawl.get("Status")
        current_start = last_crawl.get("StartTime")
        is_new_crawl = current_start is not None and (
            previous_start is None or current_start > previous_start
        )
        if state == "READY" and is_new_crawl and status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            if status != "SUCCEEDED":
                raise PipelineError(
                    f"Crawler {crawler_name!r} finished with status {status}: "
                    f"{last_crawl.get('ErrorMessage') or last_crawl.get('Message')}"
                )
            LOG.info("Crawler %s completed successfully.", crawler_name)
            return crawler
        LOG.info("Crawler %s state=%s last_crawl=%s", crawler_name, state, status)
        time.sleep(poll_seconds)
    raise PipelineError(f"Crawler {crawler_name!r} exceeded {timeout_seconds} seconds.")


def partition_exists(glue: Any, database: str, table: str, run_id: str) -> bool:
    try:
        definition = glue.get_table(DatabaseName=database, Name=table)["Table"]
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"EntityNotFoundException", "ResourceNotFoundException"}:
            raise PipelineError(f"Gold table {database}.{table} is absent from Glue Catalog.") from exc
        raise

    partition_keys = [key["Name"] for key in definition.get("PartitionKeys", [])]
    if "run_id" not in partition_keys:
        raise PipelineError(
            f"Gold table {database}.{table} is not partitioned by run_id. Keys={partition_keys}"
        )

    paginator = glue.get_paginator("get_partitions")
    expression = f"run_id='{run_id}'"
    for page in paginator.paginate(
        DatabaseName=database, TableName=table, Expression=expression
    ):
        if page.get("Partitions"):
            return True
    return False


def load_metrics_for_run(s3: Any, bucket: str, run_id: str) -> dict[str, Any]:
    key = f"observability/metrics/run_id={run_id}.json"
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        raise PipelineError(
            f"Could not read metrics for existing run_id={run_id} at s3://{bucket}/{key}."
        ) from exc
    try:
        metrics = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PipelineError(f"Metrics file for run_id={run_id} is not valid JSON.") from exc
    if metrics.get("run_id") != run_id or metrics.get("status") != "SUCCESS":
        raise PipelineError(
            f"Metrics file does not confirm a successful run: run_id={metrics.get('run_id')!r}, "
            f"status={metrics.get('status')!r}."
        )
    if not isinstance(metrics.get("gold_rows"), dict) or not metrics["gold_rows"]:
        raise PipelineError(f"Metrics file for run_id={run_id} has no Gold row counts.")
    return metrics


def wait_for_athena_query(
    athena: Any, query_id: str, timeout_seconds: int, poll_seconds: int
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        status = execution["Status"]["State"]
        LOG.info("Athena DDL status: %s", status)
        if status == "SUCCEEDED":
            return
        if status in {"FAILED", "CANCELLED"}:
            raise PipelineError(
                "Athena DDL failed: "
                + execution["Status"].get("StateChangeReason", "unknown error")
            )
        time.sleep(poll_seconds)
    raise PipelineError(f"Athena DDL exceeded {timeout_seconds} seconds.")


def repair_missing_partitions(
    athena: Any,
    database: str,
    bucket: str,
    tables: list[str],
    timeout_seconds: int,
    poll_seconds: int,
) -> list[str]:
    if not tables:
        return []
    workgroup, output_location = discover_workgroup(athena, bucket)
    repaired: list[str] = []
    for table in tables:
        if not TABLE_NAME_RE.fullmatch(table):
            raise PipelineError(f"Unsafe table name; refusing MSCK REPAIR: {table!r}")
        query = f"MSCK REPAIR TABLE {table}"
        LOG.warning(
            "Crawler did not register the current run_id partition for %s; "
            "running targeted MSCK REPAIR TABLE.", table
        )
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Catalog": "AwsDataCatalog", "Database": database},
            ResultConfiguration={"OutputLocation": output_location},
            WorkGroup=workgroup,
        )
        query_id = response["QueryExecutionId"]
        LOG.info("MSCK REPAIR TABLE %s QueryExecutionId: %s", table, query_id)
        wait_for_athena_query(athena, query_id, timeout_seconds, poll_seconds)
        repaired.append(table)
    return repaired


def discover_workgroup(athena: Any, bucket: str) -> tuple[str, str]:
    stem = bucket[:-5] if bucket.endswith("-lake") else bucket
    expected = f"{stem}-analytics"

    # Compatibilidade com Botocore sem paginador para ListWorkGroups.
    names: list[str] = []
    next_token: str | None = None

    while True:
        request: dict[str, Any] = {"MaxResults": 50}

        if next_token:
            request["NextToken"] = next_token

        page = athena.list_work_groups(**request)

        names.extend(
            group["Name"]
            for group in page.get("WorkGroups", [])
        )

        next_token = page.get("NextToken")

        if not next_token:
            break

    workgroup = expected if expected in names else "primary"
    output_location = f"s3://{bucket}/athena-results/"

    try:
        config = athena.get_work_group(
            WorkGroup=workgroup
        )["WorkGroup"]["Configuration"]

        configured = config.get(
            "ResultConfiguration", {}
        ).get("OutputLocation")

        if configured:
            output_location = configured

    except ClientError:
        if workgroup != "primary":
            raise

    return workgroup, output_location


def run_athena_validation(
    athena: Any,
    database: str,
    bucket: str,
    run_id: str,
    expected_counts: dict[str, int],
    timeout_seconds: int,
    poll_seconds: int,
) -> tuple[str, list[dict[str, Any]]]:
    tables = sorted(expected_counts)
    if not tables:
        raise PipelineError("Glue metrics contain no Gold tables to validate.")
    for table in tables:
        if not TABLE_NAME_RE.fullmatch(table):
            raise PipelineError(f"Unsafe/unexpected Gold table name in metrics: {table!r}")

    query_parts = [
        f"SELECT '{table}' AS tabela, COUNT(*) AS registros "
        f"FROM {table} WHERE run_id = '{run_id}'"
        for table in tables
    ]
    query = "\nUNION ALL\n".join(query_parts) + "\nORDER BY tabela"
    workgroup, output_location = discover_workgroup(athena, bucket)

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Catalog": "AwsDataCatalog", "Database": database},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
    )
    query_id = response["QueryExecutionId"]
    LOG.info("Athena validation QueryExecutionId: %s (workgroup=%s)", query_id, workgroup)

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        status = execution["Status"]["State"]
        LOG.info("Athena query status: %s", status)
        if status == "SUCCEEDED":
            break
        if status in {"FAILED", "CANCELLED"}:
            raise PipelineError(
                "Athena validation failed: " + execution["Status"].get("StateChangeReason", "unknown error")
            )
        time.sleep(poll_seconds)
    else:
        raise PipelineError(f"Athena query exceeded {timeout_seconds} seconds.")

    rows: list[dict[str, Any]] = []
    paginator = athena.get_paginator("get_query_results")
    first = True
    for page in paginator.paginate(QueryExecutionId=query_id):
        page_rows = page.get("ResultSet", {}).get("Rows", [])
        if first and page_rows:
            page_rows = page_rows[1:]  # header row
            first = False
        for row in page_rows:
            values = [cell.get("VarCharValue", "") for cell in row.get("Data", [])]
            if len(values) < 2:
                continue
            table, count_text = values[0], values[1]
            actual = int(count_text or "0")
            expected = int(expected_counts[table])
            rows.append(
                {
                    "table": table,
                    "actual_rows": actual,
                    "glue_metrics_rows": expected,
                    "status": "PASS" if actual == expected else "FAIL",
                }
            )

    actual_tables = {item["table"] for item in rows}
    missing = set(tables) - actual_tables
    if missing:
        raise PipelineError(f"Athena did not return results for these Gold tables: {sorted(missing)}")
    divergences = [item for item in rows if item["status"] != "PASS"]
    if divergences:
        raise PipelineError(f"Athena counts differ from Glue metrics: {divergences}")
    return query_id, rows


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if args.timeout_minutes < 1 or args.poll_seconds < 1:
        raise PipelineError("timeout-minutes and poll-seconds must be positive.")

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    sfn = session.client("stepfunctions")
    glue = session.client("glue")
    s3 = session.client("s3")
    athena = session.client("athena")
    timeout_seconds = args.timeout_minutes * 60

    state_machine_arn = discover_state_machine(sfn, args.state_machine_arn)
    env = discover_environment(sfn, glue, state_machine_arn, args.database)
    LOG.info("State machine: %s", state_machine_arn)
    LOG.info("Glue job: %s", env["job_name"])
    LOG.info("Data Catalog database: %s", env["database"])
    LOG.info("Silver crawler: %s", env["silver_crawler"])
    LOG.info("Gold crawler: %s", env["gold_crawler"])
    LOG.info("Data lake bucket: %s", env["bucket"])

    if args.existing_run_id:
        run_id = args.existing_run_id
        LOG.info(
            "Existing-run mode: skipping Step Functions and crawler execution for run_id=%s.",
            run_id,
        )
        metrics = load_metrics_for_run(s3, env["bucket"], run_id)
        silver_crawler_result = None
        gold_crawler_result = None
    else:
        execution_started_at = run_state_machine(
            sfn, state_machine_arn, timeout_seconds, args.poll_seconds
        )
        run_id, metrics = find_successful_run_metrics(
            s3, env["bucket"], execution_started_at, timeout_seconds, args.poll_seconds
        )

    gold_counts = {str(k): int(v) for k, v in metrics["gold_rows"].items()}
    missing_core = CORE_GOLD_TABLES - set(gold_counts)
    if missing_core:
        raise PipelineError(f"Glue metrics are missing mandatory Gold tables: {sorted(missing_core)}")
    if any(value < 0 for value in gold_counts.values()):
        raise PipelineError("Glue metrics contain a negative Gold row count.")
    LOG.info("Glue run_id: %s", run_id)
    LOG.info("Gold tables in run manifest: %s", ", ".join(sorted(gold_counts)))

    if not args.existing_run_id:
        crawler_timeout = timeout_seconds
        silver_crawler_result = run_crawler(
            glue, env["silver_crawler"], crawler_timeout, args.poll_seconds
        )
        gold_crawler_result = run_crawler(
            glue, env["gold_crawler"], crawler_timeout, args.poll_seconds
        )

    missing_partitions = [
        table
        for table in sorted(gold_counts)
        if not partition_exists(glue, env["database"], table, run_id)
    ]
    repaired_tables: list[str] = []
    if missing_partitions:
        repaired_tables = repair_missing_partitions(
            athena,
            env["database"],
            env["bucket"],
            missing_partitions,
            timeout_seconds,
            args.poll_seconds,
        )
        still_missing = [
            table
            for table in missing_partitions
            if not partition_exists(glue, env["database"], table, run_id)
        ]
        if still_missing:
            raise PipelineError(
                "Partitions are still absent after targeted MSCK REPAIR TABLE: "
                + ", ".join(still_missing)
            )

    query_id, validation_rows = run_athena_validation(
        athena,
        env["database"],
        env["bucket"],
        run_id,
        gold_counts,
        timeout_seconds,
        args.poll_seconds,
    )

    summary = {
        "status": "SUCCEEDED",
        "run_id": run_id,
        "region": args.region,
        "profile": args.profile,
        "state_machine_arn": state_machine_arn,
        "glue_job": env["job_name"],
        "database": env["database"],
        "bucket": env["bucket"],
        "silver_crawler": {
            "name": env["silver_crawler"],
            "last_crawl_status": (
                (silver_crawler_result.get("LastCrawl") or {}).get("Status")
                if silver_crawler_result else "SKIPPED_EXISTING_RUN_MODE"
            ),
        },
        "gold_crawler": {
            "name": env["gold_crawler"],
            "last_crawl_status": (
                (gold_crawler_result.get("LastCrawl") or {}).get("Status")
                if gold_crawler_result else "SKIPPED_EXISTING_RUN_MODE"
            ),
        },
        "repaired_partitions_tables": repaired_tables,
        "athena_query_execution_id": query_id,
        "gold_rows_total": sum(item["actual_rows"] for item in validation_rows),
        "gold_tables": validation_rows,
        "validated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"aws_validation_{run_id}.json"
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    LOG.info("Validation summary saved to %s", output_path)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (PipelineError, ClientError, BotoCoreError, OSError, ValueError) as exc:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        LOG.error("Pipeline orchestration/validation failed: %s", exc)
        raise SystemExit(1)
