from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[1]

parser = argparse.ArgumentParser(description="Publica eventos simulados de alfabetização no Kinesis.")
parser.add_argument("--stream-name", required=True)
parser.add_argument("--events", default=str(ROOT / "data" / "events" / "stream_events.jsonl"))
parser.add_argument("--delay-seconds", type=float, default=0.1)
parser.add_argument("--keep-source-timestamps", action="store_true", help="Preserva timestamps do arquivo em vez de marcar o instante real de envio.")
parser.add_argument("--region", default="sa-east-1")
args = parser.parse_args()

client = boto3.client("kinesis", region_name=args.region)
file_path = Path(args.events)
if not file_path.exists():
    raise SystemExit(f"Arquivo não encontrado: {file_path}")

sent = 0
with file_path.open(encoding="utf-8") as file:
    for line in file:
        if not line.strip():
            continue
        event = json.loads(line)
        if not args.keep_source_timestamps:
            event["event_timestamp"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        partition_key = str(event.get("id_municipio", "unknown"))
        response = client.put_record(
            StreamName=args.stream_name,
            Data=(json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"),
            PartitionKey=partition_key,
        )
        sent += 1
        print(f"Evento enviado #{sent}; shard={response.get('ShardId')}; sequence={response.get('SequenceNumber')}")
        if args.delay_seconds > 0:
            time.sleep(args.delay_seconds)
print(f"Concluído: {sent} eventos publicados. Evento simulado não deve ser apresentado como indicador oficial.")
