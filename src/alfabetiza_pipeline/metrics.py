from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .io_utils import append_jsonl, iso_now


@dataclass
class StageMetric:
    run_id: str
    stage: str
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    rows_in: int
    rows_out: int
    bytes_out: int = 0
    details: dict[str, Any] | None = None


def record_stage(
    metrics_path: Path,
    run_id: str,
    stage: str,
    rows_in: int,
    rows_out: int,
    action,
    bytes_out: int = 0,
) -> Any:
    started_at = iso_now()
    start = time.perf_counter()
    try:
        result = action()
        status = "SUCCESS"
        details = {}
        return result
    except Exception as exc:
        status = "FAILED"
        details = {"error": str(exc), "error_type": type(exc).__name__}
        raise
    finally:
        duration = round(time.perf_counter() - start, 6)
        append_jsonl(
            metrics_path,
            asdict(
                StageMetric(
                    run_id=run_id,
                    stage=stage,
                    status=status,
                    started_at=started_at,
                    finished_at=iso_now(),
                    duration_seconds=duration,
                    rows_in=int(rows_in),
                    rows_out=int(rows_out),
                    bytes_out=int(bytes_out),
                    details=details,
                )
            ),
        )
