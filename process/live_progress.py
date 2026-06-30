"""Best-effort Redis-backed live progress for controlled imports."""

from __future__ import annotations

import asyncio
import contextvars
import datetime as dt
import json
import math
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import redis

from process.import_status_events import enqueue_status_event

IMPORT_LIVE_PROGRESS_TTL_SECONDS = int(
    os.getenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_TTL_SECONDS",
        os.getenv("HLTHPRT_PTG_LIVE_PROGRESS_TTL_SECONDS", str(2 * 24 * 60 * 60)),
    )
)
IMPORT_LIVE_PROGRESS_STALE_SECONDS = int(
    os.getenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_STALE_SECONDS",
        os.getenv("HLTHPRT_PTG_LIVE_PROGRESS_STALE_SECONDS", "90"),
    )
)

_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("import_live_progress_context", default={})


@dataclass
class _LiveProgressState:
    redis_client: redis.Redis | None = None


_live_progress_state = _LiveProgressState()


def live_progress_key(run_id: str) -> str:
    """Return the Redis key used to store live progress for a run."""
    return f"import:progress:{run_id}"


def set_live_progress_context(**progress_fields: Any) -> contextvars.Token:
    """Set default live-progress fields for the current async context."""
    context_fields_dict = {
        field_key: field_value
        for field_key, field_value in progress_fields.items()
        if field_value not in (None, "")
    }
    return _context.set(context_fields_dict)


def reset_live_progress_context(token: contextvars.Token) -> None:
    """Restore the live-progress context to a previous token."""
    _context.reset(token)


def current_live_progress_context() -> dict[str, Any]:
    """Return a copy of the current async live-progress context."""
    return dict(_context.get() or {})


def write_live_progress(**progress_fields: Any) -> None:
    """Write one live-progress payload to Redis and optionally publish an event."""
    context = current_live_progress_context()
    run_id = str(progress_fields.get("run_id") or context.get("run_id") or "").strip()
    if not run_id:
        return

    now = _utc_now()
    progress_record_dict = _progress_record_from_fields(run_id, context, progress_fields, now)
    publish_event = bool(progress_record_dict.pop("publish_event", True))
    if _should_merge_previous(progress_record_dict):
        previous = _read_live_progress_payload(run_id)
        if previous:
            for key in ("importer", "source", "confidence"):
                if not progress_record_dict.get(key) or progress_record_dict.get(key) == "unknown":
                    progress_record_dict[key] = previous.get(key) or progress_record_dict.get(key)
            previous_started_at = _parse_datetime(previous.get("started_at"))
            current_started_at = _parse_datetime(progress_record_dict.get("started_at"))
            if previous_started_at is not None and (
                current_started_at is None or previous_started_at <= current_started_at
            ):
                progress_record_dict["started_at"] = previous.get("started_at")
    status = str(progress_record_dict.get("status") or "").lower()
    is_terminal = status in {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
    _normalize_progress_fields(progress_record_dict, terminal=is_terminal)
    _normalize_estimate_fields(progress_record_dict, now=now, terminal=is_terminal)
    if "label" in progress_record_dict:
        progress_record_dict["label"] = _safe_label(str(progress_record_dict["label"]))

    if publish_event:
        enqueue_status_event(
            {
                "run_id": run_id,
                "importer": progress_record_dict.get("importer"),
                "status": progress_record_dict.get("status") or "running",
                "phase_detail": str(progress_record_dict.get("phase") or "")[:128] or None,
                "progress": progress_payload_from_live(progress_record_dict),
                "estimate": estimate_payload_from_live(progress_record_dict),
                "heartbeat_at": progress_record_dict.get("updated_at"),
            }
        )

    try:
        _redis().setex(
            live_progress_key(run_id),
            IMPORT_LIVE_PROGRESS_TTL_SECONDS,
            json.dumps(progress_record_dict, default=str),
        )
    except Exception:
        return


def _progress_record_from_fields(
    run_id: str,
    context: dict[str, Any],
    progress_fields: dict[str, Any],
    now: dt.datetime,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "importer": progress_fields.get("importer") or context.get("importer") or "unknown",
        "status": progress_fields.get("status") or context.get("status") or "running",
        "source": progress_fields.get("source") or context.get("source") or "import-live-progress",
        "confidence": progress_fields.get("confidence") or context.get("confidence") or "live",
        "updated_at": now.isoformat() + "Z",
        **{context_key: context_value for context_key, context_value in context.items() if context_key != "run_id"},
        **{
            field_key: field_value
            for field_key, field_value in progress_fields.items()
            if field_value is not None
        },
    }


def enqueue_live_progress(**progress_fields: Any) -> None:
    """Schedule a live-progress write without blocking the current event loop."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        write_live_progress(**progress_fields)
        return
    try:
        loop.create_task(asyncio.to_thread(write_live_progress, **progress_fields))
    except Exception:
        return


def read_live_progress(run_id: str) -> dict[str, Any] | None:
    """Read fresh live progress for one import run, or return None."""
    run_id = str(run_id or "").strip()
    if not run_id:
        return None
    payload = _read_live_progress_payload(run_id)
    if payload is None:
        return None
    updated_at = _parse_datetime(payload.get("updated_at"))
    if updated_at is not None:
        age = (_utc_now() - updated_at).total_seconds()
        if age > max(IMPORT_LIVE_PROGRESS_STALE_SECONDS, 1):
            return None
    return payload


def _read_live_progress_payload(run_id: str) -> dict[str, Any] | None:
    try:
        raw = _redis().get(live_progress_key(run_id))
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def progress_payload_from_live(live: dict[str, Any]) -> dict[str, Any]:
    """Convert a live-progress record into an import-run progress payload."""
    progress_payload_dict = {
        "unit": live.get("unit") or "run",
        "done": live.get("done"),
        "total": live.get("total"),
        "pct": live.get("pct"),
        "message": live.get("message") or live.get("phase") or "running",
        "phase": live.get("phase"),
        "updated_at": live.get("updated_at"),
    }
    return {field_key: field_value for field_key, field_value in progress_payload_dict.items() if field_value is not None}


def estimate_payload_from_live(live: dict[str, Any]) -> dict[str, Any]:
    """Convert a live-progress record into an ETA payload."""
    estimate_payload_dict = {
        "eta_seconds": live.get("eta_seconds"),
        "estimated_finish_at": live.get("estimated_finish_at"),
        "confidence": live.get("confidence") or "live",
        "source": live.get("source") or "import-live-progress",
        "updated_at": live.get("updated_at"),
    }
    return {field_key: field_value for field_key, field_value in estimate_payload_dict.items() if field_value is not None}


def _redis() -> redis.Redis:
    if _live_progress_state.redis_client is None:
        dsn = os.getenv("HLTHPRT_REDIS_ADDRESS") or os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0"
        _live_progress_state.redis_client = redis.Redis.from_url(dsn, socket_connect_timeout=1.0, socket_timeout=1.0)
    return _live_progress_state.redis_client


def _normalize_progress_fields(merged: dict[str, Any], *, terminal: bool) -> None:
    pct = _coerce_float(merged.get("pct"))
    done = _coerce_float(merged.get("done"))
    total = _coerce_float(merged.get("total"))
    if pct is None and done is not None and total and total > 0:
        pct = (done / total) * 100.0
    if pct is not None:
        merged["pct"] = 100.0 if terminal else max(0.0, min(pct, 99.9))
    if terminal and (merged.get("done") is None) and merged.get("total") is not None:
        merged["done"] = merged.get("total")


def _should_merge_previous(merged: dict[str, Any]) -> bool:
    if merged.get("started_at"):
        return True
    if not merged.get("started_at") and merged.get("done") is not None and merged.get("total") is not None:
        return True
    return str(merged.get("importer") or "") == "unknown"


def _normalize_estimate_fields(merged: dict[str, Any], *, now: dt.datetime, terminal: bool) -> None:
    eta_seconds = _coerce_float(merged.get("eta_seconds"))
    if eta_seconds is None and not terminal:
        done = _coerce_float(merged.get("done"))
        total = _coerce_float(merged.get("total"))
        elapsed = _coerce_float(merged.get("elapsed_seconds"))
        if elapsed is None:
            started_at = _parse_datetime(merged.get("started_at"))
            if started_at is not None:
                elapsed = max((now - started_at).total_seconds(), 0.0)
        if done is not None and total is not None and total > done > 0 and elapsed and elapsed > 0:
            eta_seconds = (total - done) * (elapsed / done)
    if eta_seconds is not None and eta_seconds >= 0 and math.isfinite(eta_seconds):
        merged["eta_seconds"] = eta_seconds
        merged["estimated_finish_at"] = (now + dt.timedelta(seconds=eta_seconds)).isoformat() + "Z"


def _safe_label(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme and parsed.netloc:
        path_tail = parsed.path.rsplit("/", 1)[-1]
        return f"{parsed.netloc}/{path_tail}" if path_tail else parsed.netloc
    return value[:256]


def _coerce_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _utc_now() -> dt.datetime:
    return dt.datetime.utcnow()


def _parse_datetime(value: Any) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed
