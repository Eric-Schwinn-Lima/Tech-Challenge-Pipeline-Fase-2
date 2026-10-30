"""SQS consumer: persist raw events in S3 and measure event ingestion latency."""
import datetime as dt
import json
import os
import uuid

import boto3

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")
BUCKET = os.environ["BUCKET_NAME"]
PREFIX = os.environ.get("STREAM_PREFIX", "bronze/streaming").strip("/")


def lambda_handler(event, context):
    now = dt.datetime.now(dt.timezone.utc)
    date_path = now.strftime("ingest_date=%Y-%m-%d/hour=%H")
    rows = []
    latency_values = []
    invalid_count = 0

    for record in event.get("Records", []):
        raw = record.get("body", "")
        try:
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                raise ValueError("event_body_must_be_json_object")
            obj.setdefault("origem_dado", "simulado")
            obj["ingested_at"] = now.isoformat().replace("+00:00", "Z")
            obj["sqs_message_id"] = record.get("messageId")

            timestamp_text = str(obj.get("event_timestamp", ""))
            produced = dt.datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
            if produced.tzinfo is None:
                produced = produced.replace(tzinfo=dt.timezone.utc)
            latency = (now - produced.astimezone(dt.timezone.utc)).total_seconds()
            obj["latencia_segundos"] = max(0.0, latency)
            if latency < 0:
                obj["validation_error"] = "event_timestamp_in_future"
            latency_values.append(max(0.0, latency))
        except Exception as exc:
            invalid_count += 1
            obj = {
                "raw_event": raw,
                "validation_error": f"invalid_json_or_timestamp:{type(exc).__name__}",
                "origem_dado": "simulado",
                "ingested_at": now.isoformat().replace("+00:00", "Z"),
                "sqs_message_id": record.get("messageId"),
            }
        rows.append(json.dumps(obj, ensure_ascii=False))

    if rows:
        key = f"{PREFIX}/{date_path}/batch-{uuid.uuid4().hex}.jsonl"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=("\n".join(rows) + "\n").encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        metrics = [
            {"MetricName": "EventsIngested", "Value": len(rows), "Unit": "Count"},
            {"MetricName": "InvalidEvents", "Value": invalid_count, "Unit": "Count"},
        ]
        if latency_values:
            metrics.append({
                "MetricName": "EventLatencySeconds",
                "Value": sum(latency_values) / len(latency_values),
                "Unit": "Seconds",
            })
        cloudwatch.put_metric_data(Namespace="AlfabetizacaoPipeline", MetricData=metrics)

    return {"records_received": len(rows), "invalid_events": invalid_count}
