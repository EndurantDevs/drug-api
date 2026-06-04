from __future__ import annotations

import datetime
import json
import os
import uuid
from typing import Any

import msgpack
from arq import create_pool
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db.models import db
from process.import_status_events import enqueue_status_event
from process.live_progress import enqueue_live_progress, estimate_payload_from_live, progress_payload_from_live, read_live_progress
from process.redis_config import redis_settings

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
    return datetime.datetime.utcnow()


def _schema() -> str:
    return os.getenv("DB_SCHEMA") or "rx_data"


async def ensure_import_run_table() -> None:
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
    return {"engine": ENGINE_NAME, "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"), "status": "ok", "time": utc_now().isoformat(), "features": {"control_api": True, "enqueue_adapters": True, "enqueue_adapter_count": len(_IMPORTERS)}}


async def create_import_run(payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    importer = str(payload.get("importer") or "").strip()
    spec = _IMPORTERS.get(importer)
    if spec is None:
        raise ValueError(f"unknown importer: {importer}")
    await ensure_import_run_table()
    schema = _schema()
    idempotency_key = str(payload.get("idempotency_key") or "").strip() or None
    if idempotency_key:
        active = await _find_active_by_idempotency_key(idempotency_key)
        if active:
            return active, False
    now = utc_now()
    run_id = str(payload.get("run_id") or "").strip() or f"run_{uuid.uuid4().hex}"
    row = {
        "run_id": run_id,
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "importer": importer,
        "family": spec["family"],
        "status": "queued",
        "phase_detail": "created",
        "params": payload.get("params") if isinstance(payload.get("params"), dict) else {},
        "idempotency_key": idempotency_key,
        "triggered_by": str(payload.get("triggered_by") or "api"),
        "schedule_id": payload.get("schedule_id"),
        "created_at": now,
        "heartbeat_at": now,
        "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued"},
        "metrics": {},
        "error": None,
        "import_id": payload.get("import_id"),
        "retry_of_run_id": payload.get("retry_of_run_id"),
    }
    try:
        db_row = {**row, **_json_fields(row)}
        await db.status(
            text(
                f"""
            INSERT INTO {schema}.import_run
                (run_id, engine, node_id, importer, family, status, phase_detail, params, idempotency_key,
                 triggered_by, schedule_id, created_at, heartbeat_at, progress, metrics, error, import_id, retry_of_run_id)
            VALUES
                (:run_id, :engine, :node_id, :importer, :family, :status, :phase_detail, :params, :idempotency_key,
                 :triggered_by, :schedule_id, :created_at, :heartbeat_at, :progress, :metrics, :error, :import_id, :retry_of_run_id)
            """
            ),
            **db_row,
        )
    except IntegrityError:
        if idempotency_key:
            active = await _find_active_by_idempotency_key(idempotency_key)
            if active:
                return active, False
        raise
    enqueue = await _enqueue(spec, row)
    db_enqueue = {**enqueue, **_json_fields(enqueue)}
    await db.status(
        text(
            f"""
        UPDATE {schema}.import_run
           SET status = :status, phase_detail = :phase_detail, heartbeat_at = :heartbeat_at,
               progress = :progress, metrics = :metrics, error = :error
         WHERE run_id = :run_id
        """
        ),
        run_id=run_id,
        **db_enqueue,
    )
    result = {**row, **enqueue}
    enqueue_status_event(result)
    _write_run_live_progress(result, publish_event=False)
    return result, True


async def _enqueue(spec: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    params = row["params"] if isinstance(row.get("params"), dict) else {}
    test_mode = bool(params.get("test_mode", params.get("test", False)))
    task = {
        "run_id": row["run_id"],
        "importer": row.get("importer"),
        "family": row.get("family"),
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
        job = await redis.enqueue_job(spec["function"], task, _queue_name=spec["queue"])
    except Exception as exc:
        return {"status": "failed", "phase_detail": "enqueue failed", "heartbeat_at": utc_now(), "progress": {"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "enqueue failed"}, "metrics": {"queue": spec["queue"], "function": spec["function"]}, "error": {"code": "enqueue_failed", "message": str(exc)}}
    return {"status": "queued", "phase_detail": "enqueued", "heartbeat_at": utc_now(), "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued"}, "metrics": {"queue": spec["queue"], "function": spec["function"], "job_id": getattr(job, "job_id", None) or str(job)}, "error": None}


async def list_import_runs(status: str | None = None, importer: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    await ensure_import_run_table()
    schema = _schema()
    where = []
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 50), 200))}
    if status:
        where.append("status = :status")
        params["status"] = status
    if importer:
        where.append("importer = :importer")
        params["importer"] = importer
    sql = f"SELECT * FROM {schema}.import_run"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC LIMIT :limit"
    rows = await db.all(text(sql), **params)
    return [_overlay_live_progress(_row_to_dict(row)) for row in rows]


async def get_import_run(run_id: str) -> dict[str, Any] | None:
    await ensure_import_run_table()
    row = await db.first(text(f"SELECT * FROM {_schema()}.import_run WHERE run_id = :run_id LIMIT 1"), run_id=run_id)
    return _overlay_live_progress(_row_to_dict(row)) if row else None


async def request_cancel(run_id: str) -> dict[str, Any] | None:
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
        progress = {"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "canceled"}
        metrics = {**(run.get("metrics") or {}), "cancel_signal": cancel_signal}
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
            progress=json.dumps(progress),
            metrics=json.dumps(metrics),
        )
        updated = await get_import_run(run_id)
        if updated:
            _write_run_live_progress({**updated, "progress": progress}, publish_event=False)
            enqueue_status_event({**updated, "progress": progress, "metrics": metrics})
        return updated
    progress = {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "cancel requested"}
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
        progress=json.dumps(progress),
    )
    updated = await get_import_run(run_id)
    if updated:
        _write_run_live_progress({**updated, "progress": progress}, publish_event=False)
        enqueue_status_event({**updated, "progress": progress})
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
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"redis": False, "removed": False, "error": str(exc), "queue": queue, "job_id": job_id}


async def retry_import_run(run_id: str, payload: dict[str, Any]) -> tuple[dict[str, Any], bool] | None:
    current = await get_import_run(run_id)
    if not current:
        return None
    retry_payload = {"importer": current["importer"], "params": current.get("params") or {}, "triggered_by": payload.get("triggered_by") or "api", "retry_of_run_id": run_id}
    return await create_import_run(retry_payload)


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
                pass
    return data


def _overlay_live_progress(data: dict[str, Any]) -> dict[str, Any]:
    if data.get("status") not in ACTIVE_STATUSES:
        return data
    live = read_live_progress(str(data.get("run_id") or ""))
    if not live:
        return data
    result = dict(data)
    result["progress"] = {**dict(result.get("progress") or {}), **progress_payload_from_live(live)}
    estimate = estimate_payload_from_live(live)
    if estimate:
        result["estimate"] = estimate
    phase = live.get("phase") or result.get("phase_detail")
    if phase:
        result["phase_detail"] = str(phase)[:128]
    return result


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


def _json_fields(values: dict[str, Any]) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for key in ("params", "progress", "metrics", "error"):
        if key in values:
            value = values[key]
            result[key] = None if value is None else json.dumps(value)
    return result
