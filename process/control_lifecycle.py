from __future__ import annotations

import asyncio
import datetime
import json
import os
from contextlib import suppress
from importlib import import_module
from inspect import signature
from typing import Any

from sqlalchemy import text

from db.models import db
from process.import_status_events import enqueue_status_event, flush_status_events
from process.live_progress import enqueue_live_progress, reset_live_progress_context, set_live_progress_context


async def control_single_job_start(ctx: dict[str, Any], task: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = task if isinstance(task, dict) else {}
    run_id = str(payload.get("run_id") or "").strip()
    importer = str(payload.get("importer") or payload.get("target_function") or "unknown").strip()
    target_module = str(payload.get("target_module") or "").strip()
    target_function = str(payload.get("target_function") or "").strip()
    call_style = str(payload.get("call_style") or "ctx_task").strip()
    target_task = payload.get("task") if isinstance(payload.get("task"), dict) else {}
    if run_id:
        target_task = {**target_task, "run_id": run_id}
        ctx["control_run_id"] = run_id
    if not target_module or not target_function:
        await mark_control_run(run_id, status="failed", phase_detail="control target missing", progress_message="target missing")
        raise RuntimeError("target_module and target_function are required")

    started_at = datetime.datetime.utcnow().isoformat()
    live_token = None
    heartbeat_task = None
    if run_id:
        live_token = set_live_progress_context(
            run_id=run_id,
            importer=importer,
            status="running",
            started_at=started_at,
            source="import-control-heartbeat",
            confidence="heartbeat",
        )
        heartbeat_task = asyncio.create_task(_live_progress_heartbeat(run_id, importer, target_function, started_at))

    await mark_control_run(run_id, status="running", phase_detail=f"{target_function} running", progress_message="running")
    try:
        module = import_module(target_module)
        fn = getattr(module, target_function)
        if call_style == "kwargs":
            accepted = signature(fn).parameters
            kwargs = {key: value for key, value in target_task.items() if key in accepted}
            result = await fn(**kwargs)
            terminal_progress = _terminal_progress_from_result(target_function, result)
            terminal_metrics = result if isinstance(result, dict) else None
            await mark_control_run(
                run_id,
                status="succeeded",
                phase_detail=f"{target_function} succeeded",
                progress_message="succeeded",
                metrics=terminal_metrics,
                progress=terminal_progress,
            )
            await _flush_terminal_status_events()
            return {"status": "succeeded", "run_id": run_id, "result": result}
        accepted = signature(fn).parameters
        if len(accepted) >= 2:
            result = await fn(ctx, target_task)
        else:
            result = await fn(ctx)
    except Exception as exc:
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail=f"{target_function} failed",
            progress_message="failed",
            error={"code": "import_failed", "message": str(exc)},
        )
        await _flush_terminal_status_events()
        raise
    finally:
        await _stop_live_progress_heartbeat(heartbeat_task)
        if live_token is not None:
            reset_live_progress_context(live_token)
    return {"status": "running", "run_id": run_id, "result": result}


async def _live_progress_heartbeat(run_id: str, importer: str, target_function: str, started_at: str) -> None:
    interval = float(os.getenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "15"))
    if interval <= 0:
        return
    phase = f"{target_function} running"
    while True:
        await asyncio.sleep(interval)
        enqueue_live_progress(
            run_id=run_id,
            importer=importer,
            status="running",
            phase=phase,
            unit="run",
            done=0,
            total=1,
            pct=0,
            message="running",
            started_at=started_at,
            source="import-control-heartbeat",
            confidence="heartbeat",
        )


async def _stop_live_progress_heartbeat(task: asyncio.Task | None) -> None:
    if task is None or task.done():
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


def _terminal_progress_from_result(target_function: str, result: Any) -> dict[str, Any] | None:
    if not isinstance(result, dict):
        return None
    if target_function == "import_drug_indications" and result.get("labels_scanned") is not None:
        scanned = int(result.get("labels_scanned") or 0)
        return {
            "unit": "labels",
            "done": scanned,
            "total": scanned,
            "pct": 100,
            "message": "succeeded",
            "phase": "drug indications import published" if result.get("published") else "drug indications import staged",
        }
    return None


async def _flush_terminal_status_events() -> None:
    timeout = float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TERMINAL_FLUSH_SECONDS", "0.25"))
    if timeout <= 0:
        return
    await flush_status_events(timeout_seconds=timeout)


async def mark_control_run(
    run_id: str,
    *,
    status: str,
    phase_detail: str,
    progress_message: str,
    error: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
    progress: dict[str, Any] | None = None,
) -> None:
    if not run_id:
        return
    await ensure_import_run_table()
    schema = os.getenv("DB_SCHEMA") or "rx_data"
    now = datetime.datetime.utcnow()
    finished_at = now if status in {"succeeded", "failed", "canceled", "dead_letter"} else None
    started_at = now if status == "running" else None
    progress_payload = progress or {"unit": "run", "total": 1, "done": 1 if finished_at else 0, "pct": 100 if finished_at else 0, "message": progress_message}
    guarded_status_update = status in {"running", "succeeded", "failed", "dead_letter"}
    cancel_guard = "AND status NOT IN ('canceling', 'canceled')" if guarded_status_update else ""
    await db.status(
        text(
            f"""
        UPDATE {schema}.import_run
           SET status = :status,
               phase_detail = :phase_detail,
               started_at = COALESCE(started_at, :started_at),
               heartbeat_at = :heartbeat_at,
               finished_at = COALESCE(finished_at, :finished_at),
               progress = :progress,
               metrics = COALESCE(:metrics, metrics),
               error = :error
         WHERE run_id = :run_id
           {cancel_guard}
        """
        ),
        run_id=run_id,
        status=status,
        phase_detail=phase_detail,
        started_at=started_at,
        heartbeat_at=now,
        finished_at=finished_at,
        progress=json.dumps(progress_payload),
        metrics=None if metrics is None else json.dumps(metrics),
        error=None if error is None else json.dumps(error),
    )
    live_payload = {
        **progress_payload,
        "run_id": run_id,
        "status": status,
        "phase": progress_payload.get("phase") or phase_detail,
        "message": progress_payload.get("message") or progress_message,
        "started_at": started_at.isoformat() if started_at else None,
        "finished_at": finished_at.isoformat() if finished_at else None,
        "publish_event": False,
    }
    enqueue_live_progress(**live_payload)
    enqueue_status_event(
        {
            "run_id": run_id,
            "status": status,
            "phase_detail": phase_detail,
            "progress": progress_payload,
            "metrics": metrics or {},
            "error": error,
            "heartbeat_at": now.isoformat(),
            "started_at": started_at.isoformat() if started_at else None,
            "finished_at": finished_at.isoformat() if finished_at else None,
        }
    )


async def ensure_import_run_table() -> None:
    schema = os.getenv("DB_SCHEMA") or "rx_data"
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
