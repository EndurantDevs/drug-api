from __future__ import annotations

import datetime
import json
import os
import uuid
from contextlib import suppress
from typing import Any

import msgpack
import redis
from arq import create_pool
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from api.control_run_store import insert_import_run, update_import_run_after_enqueue
from db.models import db
from process.import_status_events import enqueue_status_event
from process.live_progress import (enqueue_live_progress, estimate_payload_from_live, progress_payload_from_live,
                                   read_live_progress)
from process.redis_config import redis_dsn, redis_settings

ENGINE_NAME = "drug-api"
ACTIVE_STATUSES = {"queued", "starting", "running", "finalizing", "canceling"}
NDC_QUEUE_NAME = (
    os.environ.get("HLTHPRT_ARQ_QUEUE_NDC")
    or os.environ.get("ARQ_QUEUE_NDC")
    or "arq:queue:drug-api-import-ndc"
)
LABEL_QUEUE_NAME = (
    os.environ.get("HLTHPRT_ARQ_QUEUE_LABEL")
    or os.environ.get("ARQ_QUEUE_LABEL")
    or "arq:queue:drug-api-import-label"
)

_IMPORTERS: dict[str, dict[str, Any]] = {
    "ndc": {
        "family": "drug",
        "queue": NDC_QUEUE_NAME,
        "function": "control_single_job_start",
        "target_module": "process.ndc_product",
        "target_function": "init_file",
        "call_style": "ctx_task",
    },
    "label": {
        "family": "drug",
        "queue": LABEL_QUEUE_NAME,
        "function": "control_single_job_start",
        "target_module": "process.label",
        "target_function": "init_label_file",
        "call_style": "ctx_task",
    },
    "drug-indications": {
        "family": "drug",
        "queue": "arq:queue:drug-api-import-indications",
        "function": "control_single_job_start",
        "target_module": "process.drug_indications",
        "target_function": "import_drug_indications",
        "call_style": "kwargs",
    },
}


def utc_now() -> datetime.datetime:
    """Return the current UTC timestamp for import-control records."""
    return datetime.datetime.utcnow()


def _schema() -> str:
    return os.getenv("DB_SCHEMA") or "rx_data"


async def ensure_import_run_table() -> None:
    """Create the import-run state table and active-run idempotency index."""
    schema = _schema()
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    await db.status(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.import_run (
            run_id VARCHAR PRIMARY KEY,
            engine VARCHAR NOT NULL,
            node_id VARCHAR,
            importer VARCHAR NOT NULL,
            family VARCHAR,
            status VARCHAR NOT NULL,
            phase_detail TEXT,
            params JSONB DEFAULT '{{}}'::jsonb,
            idempotency_key VARCHAR,
            triggered_by VARCHAR,
            schedule_id VARCHAR,
            created_at TIMESTAMP,
            started_at TIMESTAMP,
            heartbeat_at TIMESTAMP,
            finished_at TIMESTAMP,
            progress JSONB DEFAULT '{{}}'::jsonb,
            metrics JSONB DEFAULT '{{}}'::jsonb,
            error JSONB,
            import_id VARCHAR,
            retry_of_run_id VARCHAR
        );
        """
    )
    await db.status(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS import_run_active_idempotency_idx
            ON {schema}.import_run (idempotency_key)
            WHERE idempotency_key IS NOT NULL
              AND status IN ('queued', 'starting', 'running', 'finalizing', 'canceling');
        """
    )


def importer_registry() -> list[dict[str, Any]]:
    """Return importers this service can expose to import-control clients."""
    return [
        {
            "name": name,
            "engine": ENGINE_NAME,
            "family": spec["family"],
            "kind": "scheduled",
            "lifecycle": "single",
            "schedulable": True,
            "cancelable": False,
            "retryable": True,
            "enqueue_adapter": "arq_single_job",
            "queue": spec["queue"],
            "params_schema": _params_schema(name),
        }
        for name, spec in sorted(_IMPORTERS.items())
    ]


def _params_schema(name: str) -> list[dict[str, Any]]:
    if name in {"ndc", "label"}:
        return [
            {"name": "test", "opts": ["--test"], "required": False, "multiple": False, "is_flag": True, "type": "boolean", "default": False, "help": "Process the first FDA partition with a bounded row sample."},
            {"name": "max_records", "opts": ["--max-records"], "required": False, "multiple": False, "is_flag": False, "type": "integer", "default": 5000, "help": "Maximum records to parse in test mode."},
        ]
    if name != "drug-indications":
        return []
    return [
        {"name": "test", "opts": ["--test"], "required": False, "multiple": False, "is_flag": True, "type": "boolean", "default": False, "help": "Process a small label sample for a quick smoke run."},
        {"name": "import_id", "opts": ["--import-id"], "required": False, "multiple": False, "is_flag": False, "type": "text", "default": None, "help": "Override import id/date suffix for table names."},
    ]


def node_health() -> dict[str, Any]:
    """Return runtime health, queue depth, and local worker status."""
    return {
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "status": "ok",
        "time": utc_now().isoformat(),
        "features": {
            "control_api": True,
            "enqueue_adapters": True,
            "enqueue_adapter_count": len(_IMPORTERS),
        },
        "queue_depth": _queue_depths(),
        "workers": _worker_health(),
    }


def _worker_health() -> dict[str, Any]:
    try:
        from api.control_workers import worker_registry

        return {
            item["queue"]: {
                "worker_class": item["worker_class"],
                "role": item["role"],
                "running": item["running"],
                "pid": item.get("pid"),
            }
            for item in worker_registry()
        }
    except Exception:
        return {}


def _queue_depths() -> dict[str, int]:
    queues = {
        str(spec.get("queue"))
        for spec in _IMPORTERS.values()
        if str(spec.get("queue") or "").strip()
    }
    try:
        client = redis.Redis.from_url(redis_dsn(), socket_connect_timeout=1.0, socket_timeout=1.0)
        return {queue: int(client.zcard(queue) or 0) for queue in sorted(queues)}
    except Exception:
        return {}


async def create_import_run(run_request: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Persist and enqueue one import run request."""
    importer = str(run_request.get("importer") or "").strip()
    spec = _IMPORTERS.get(importer)
    if spec is None:
        raise ValueError(f"unknown importer: {importer}")
    await ensure_import_run_table()
    schema = _schema()
    idempotency_key = str(run_request.get("idempotency_key") or "").strip() or None
    if idempotency_key:
        active = await _find_active_by_idempotency_key(idempotency_key)
        if active:
            return active, False
    now = utc_now()
    run_id = str(run_request.get("run_id") or "").strip() or f"run_{uuid.uuid4().hex}"
    run_record_dict = _run_record_from_request(run_request, importer, spec, run_id, idempotency_key, now)
    try:
        await insert_import_run(schema, run_record_dict)
    except IntegrityError:
        if idempotency_key:
            active = await _find_active_by_idempotency_key(idempotency_key)
            if active:
                return active, False
        raise
    enqueue_update = await _enqueue(spec, run_record_dict)
    await update_import_run_after_enqueue(schema, run_id, enqueue_update)
    created_run_dict = {**run_record_dict, **enqueue_update}
    enqueue_status_event(created_run_dict)
    _write_run_live_progress(created_run_dict, publish_event=False)
    return created_run_dict, True


def _run_record_from_request(
    run_request: dict[str, Any],
    importer: str,
    spec: dict[str, Any],
    run_id: str,
    idempotency_key: str | None,
    now: datetime.datetime,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "importer": importer,
        "family": spec["family"],
        "status": "queued",
        "phase_detail": "created",
        "params": run_request.get("params") if isinstance(run_request.get("params"), dict) else {},
        "idempotency_key": idempotency_key,
        "triggered_by": str(run_request.get("triggered_by") or "api"),
        "schedule_id": run_request.get("schedule_id"),
        "created_at": now,
        "heartbeat_at": now,
        "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued"},
        "metrics": {},
        "error": None,
        "import_id": run_request.get("import_id"),
        "retry_of_run_id": run_request.get("retry_of_run_id"),
    }


async def _enqueue(spec: dict[str, Any], run_record_dict: dict[str, Any]) -> dict[str, Any]:
    params = run_record_dict["params"] if isinstance(run_record_dict.get("params"), dict) else {}
    test_mode = bool(params.get("test_mode", params.get("test", False)))
    job_payload_dict = {
        "run_id": run_record_dict["run_id"],
        "importer": run_record_dict.get("importer"),
        "family": run_record_dict.get("family"),
        "target_module": spec["target_module"],
        "target_function": spec["target_function"],
        "call_style": spec["call_style"],
        "task": {"test_mode": test_mode, **params},
    }
    try:
        redis = await create_pool(
            redis_settings(),
            default_queue_name=spec["queue"],
            job_serializer=msgpack.packb,
            job_deserializer=lambda b: msgpack.unpackb(b, raw=False),
        )
        job = await redis.enqueue_job(spec["function"], job_payload_dict, _queue_name=spec["queue"])
    except Exception as exc:
        return {"status": "failed", "phase_detail": "enqueue failed", "heartbeat_at": utc_now(), "progress": {"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "enqueue failed"}, "metrics": {"queue": spec["queue"], "function": spec["function"]}, "error": {"code": "enqueue_failed", "message": str(exc)}}
    return {"status": "queued", "phase_detail": "enqueued", "heartbeat_at": utc_now(), "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued"}, "metrics": {"queue": spec["queue"], "function": spec["function"], "job_id": getattr(job, "job_id", None) or str(job)}, "error": None}


async def list_import_runs(status: str | None = None, importer: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """Return recent import runs, optionally filtered by status or importer."""
    await ensure_import_run_table()
    schema = _schema()
    where_clauses = []
    query_params_dict: dict[str, Any] = {"limit": max(1, min(int(limit or 50), 200))}
    if status:
        where_clauses.append("status = :status")
        query_params_dict["status"] = status
    if importer:
        where_clauses.append("importer = :importer")
        query_params_dict["importer"] = importer
    sql = f"SELECT * FROM {schema}.import_run"
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    sql += " ORDER BY created_at DESC LIMIT :limit"
    rows = await db.all(text(sql), **query_params_dict)
    return [_overlay_live_progress(_row_to_dict(run_row)) for run_row in rows]


async def get_import_run(run_id: str) -> dict[str, Any] | None:
    """Return one import run by id with live progress overlaid when available."""
    await ensure_import_run_table()
    row = await db.first(text(f"SELECT * FROM {_schema()}.import_run WHERE run_id = :run_id LIMIT 1"), run_id=run_id)
    return _overlay_live_progress(_row_to_dict(row)) if row else None


async def request_cancel(run_id: str) -> dict[str, Any] | None:
    """Request cancellation for a queued or actively cancelable import run."""
    run = await get_import_run(run_id)
    if not run:
        return None
    if run["status"] in {"succeeded", "failed", "canceled", "dead_letter"}:
        return run
    if run["status"] != "queued" and not _supports_active_cancel(str(run.get("importer") or "")):
        raise ValueError(f"importer does not support canceling active runs: {run.get('importer')}")
    now = utc_now()
    if run["status"] == "queued":
        cancel_signal = await _remove_queued_job(run)
        progress_payload_dict = {"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "canceled"}
        metrics_payload_dict = {**(run.get("metrics") or {}), "cancel_signal": cancel_signal}
        await db.status(
            text(
                f"""
            UPDATE {_schema()}.import_run
               SET status = 'canceled',
                   phase_detail = 'canceled before start',
                   heartbeat_at = :now,
                   finished_at = :now,
                   progress = :progress,
                   metrics = :metrics
             WHERE run_id = :run_id
            """
            ),
            run_id=run_id,
            now=now,
            progress=json.dumps(progress_payload_dict),
            metrics=json.dumps(metrics_payload_dict),
        )
        updated = await get_import_run(run_id)
        if updated:
            _write_run_live_progress({**updated, "progress": progress_payload_dict}, publish_event=False)
            enqueue_status_event({**updated, "progress": progress_payload_dict, "metrics": metrics_payload_dict})
        return updated
    progress_payload_dict = {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "cancel requested"}
    await db.status(
        text(
            f"""
        UPDATE {_schema()}.import_run
           SET status = 'canceling', phase_detail = 'cancel requested', heartbeat_at = :now,
               progress = :progress
         WHERE run_id = :run_id
        """
        ),
        run_id=run_id,
        now=now,
        progress=json.dumps(progress_payload_dict),
    )
    updated = await get_import_run(run_id)
    if updated:
        _write_run_live_progress({**updated, "progress": progress_payload_dict}, publish_event=False)
        enqueue_status_event({**updated, "progress": progress_payload_dict})
    return updated


def _supports_active_cancel(importer: str) -> bool:
    return bool(_IMPORTERS.get(importer, {}).get("cancelable"))


async def _remove_queued_job(run: dict[str, Any]) -> dict[str, Any]:
    metrics = run.get("metrics") if isinstance(run.get("metrics"), dict) else {}
    queue = str(metrics.get("queue") or "").strip()
    job_id = str(metrics.get("job_id") or "").strip()
    if not queue or not job_id:
        return {"redis": False, "removed": False, "reason": "missing queue or job_id"}
    try:
        redis = await create_pool(
            redis_settings(),
            default_queue_name=queue,
            job_serializer=msgpack.packb,
            job_deserializer=lambda b: msgpack.unpackb(b, raw=False),
        )
        removed = int(await redis.zrem(queue, job_id) or 0)
        deleted = int(await redis.delete(f"arq:job:{job_id}") or 0)
        return {"redis": True, "removed": removed > 0, "queue": queue, "job_id": job_id, "deleted_job_key": deleted > 0}
    except Exception as exc:
        return {"redis": False, "removed": False, "error": str(exc), "queue": queue, "job_id": job_id}


async def retry_import_run(run_id: str, retry_request: dict[str, Any]) -> tuple[dict[str, Any], bool] | None:
    """Create a new import run from an existing run's importer and params."""
    current = await get_import_run(run_id)
    if not current:
        return None
    retry_request_dict = {
        "importer": current["importer"],
        "params": current.get("params") or {},
        "triggered_by": retry_request.get("triggered_by") or "api",
        "retry_of_run_id": run_id,
    }
    return await create_import_run(retry_request_dict)


async def _find_active_by_idempotency_key(idempotency_key: str) -> dict[str, Any] | None:
    row = await db.first(
        text(f"SELECT * FROM {_schema()}.import_run WHERE idempotency_key = :idempotency_key AND status = ANY(:statuses) LIMIT 1"),
        idempotency_key=idempotency_key,
        statuses=list(ACTIVE_STATUSES),
    )
    return _row_to_dict(row) if row else None


def _row_to_dict(row: Any) -> dict[str, Any]:
    data = dict(row)
    for key in ("created_at", "started_at", "heartbeat_at", "finished_at"):
        value = data.get(key)
        if hasattr(value, "isoformat"):
            data[key] = value.isoformat()
    for key in ("params", "progress", "metrics", "error"):
        value = data.get(key)
        if isinstance(value, str):
            try:
                data[key] = json.loads(value)
            except json.JSONDecodeError:
                continue
    return data


def _overlay_live_progress(data: dict[str, Any]) -> dict[str, Any]:
    if data.get("status") not in ACTIVE_STATUSES:
        return data
    live = read_live_progress(str(data.get("run_id") or ""))
    if not live:
        return data
    run_payload_dict = dict(data)
    run_payload_dict["progress"] = {**dict(run_payload_dict.get("progress") or {}), **progress_payload_from_live(live)}
    estimate = estimate_payload_from_live(live)
    if estimate:
        run_payload_dict["estimate"] = estimate
    phase = live.get("phase") or run_payload_dict.get("phase_detail")
    if phase:
        run_payload_dict["phase_detail"] = str(phase)[:128]
    return run_payload_dict


def _write_run_live_progress(run: dict[str, Any], *, publish_event: bool) -> None:
    progress = run.get("progress") if isinstance(run.get("progress"), dict) else {}
    payload = dict(progress)
    payload.update(
        run_id=run.get("run_id"),
        importer=run.get("importer"),
        status=run.get("status"),
        started_at=run.get("started_at"),
        finished_at=run.get("finished_at"),
        publish_event=publish_event,
    )
    payload.setdefault("phase", run.get("phase_detail"))
    payload.setdefault("message", run.get("phase_detail"))
    enqueue_live_progress(**payload)
