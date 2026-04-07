import json
import hashlib
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional


CURRENT_PAYLOAD_VERSION = "v1"


def json_serial(obj: Any) -> Any:
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def idempotency_key(*parts: Any) -> str:
    joined = "|".join(str(p if p is not None else "") for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


@dataclass
class QueueEnvelope:
    run_id: str
    trace_id: str
    stage: str
    payload_version: str
    attempt: int
    created_at: str
    message_id: str
    notice_id: str
    source_file: str
    row_index: int
    data: Dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=json_serial, ensure_ascii=True)

    @staticmethod
    def from_json(payload: str) -> "QueueEnvelope":
        parsed = json.loads(payload)
        required = {
            "run_id",
            "trace_id",
            "stage",
            "payload_version",
            "attempt",
            "created_at",
            "message_id",
            "notice_id",
            "source_file",
            "row_index",
            "data",
        }
        missing = sorted(required - set(parsed.keys()))
        if missing:
            raise ValueError(f"Envelope missing required fields: {missing}")

        nid_v = parsed.get("notice_id")
        notice_id = str(nid_v) if nid_v is not None else ""
        sf_v = parsed.get("source_file")
        source_file = str(sf_v) if sf_v is not None else ""
        ri_v = parsed.get("row_index")
        row_index = int(ri_v) if ri_v is not None else 0
        return QueueEnvelope(
            run_id=str(parsed["run_id"]),
            trace_id=str(parsed["trace_id"]),
            stage=str(parsed["stage"]),
            payload_version=str(parsed["payload_version"]),
            attempt=int(parsed["attempt"]),
            created_at=str(parsed["created_at"]),
            message_id=str(parsed["message_id"]),
            notice_id=notice_id,
            source_file=source_file,
            row_index=row_index,
            data=parsed.get("data") if isinstance(parsed.get("data"), dict) else {},
        )


def make_envelope(
    *,
    run_id: str,
    trace_id: str,
    stage: str,
    notice_id: str,
    source_file: str,
    row_index: int,
    data: Dict[str, Any],
    attempt: int = 1,
    message_id: Optional[str] = None,
) -> QueueEnvelope:
    msg_id = message_id if message_id is not None else idempotency_key(stage, notice_id, source_file, row_index)
    return QueueEnvelope(
        run_id=run_id,
        trace_id=trace_id,
        stage=stage,
        payload_version=CURRENT_PAYLOAD_VERSION,
        attempt=attempt,
        created_at=utc_now_iso(),
        message_id=msg_id,
        notice_id=notice_id if notice_id is not None else "",
        source_file=source_file if source_file is not None else "",
        row_index=row_index,
        data=data if data is not None else {},
    )
