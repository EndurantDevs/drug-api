from __future__ import annotations

import asyncio
import datetime
import json
import os
from contextlib import suppress
from dataclasses import dataclass
from importlib import import_module
from inspect import signature
from typing import Any

from sqlalchemy import text

from db.models import db
from process.import_status_events import enqueue_status_event, flush_status_events
from process.live_progress import enqueue_live_progress, reset_live_progress_context, set_live_progress_context


@dataclass(frozen=True)
class ControlTask:
    run_id: str
    importer: str
    target_module: str
    target_function: str
    call_style: str
    target_task_dict: dict[str, Any]


@dataclass(frozen=True)
class ControlRunTransition:
    run_id: str
    status: str
    phase_detail: str
    progress_message: str
    metrics: dict[str, Any] | None
    error: dict[str, Any] | None
    now: datetime.datetime
    started_at: datetime.datetime | None
    finished_at: datetime.datetime | None


async def control_single_job_start(ctx: dict[str, Any], task: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run one import-control job target and keep import_run state current."""
    control_task = _control_task_from_payload(ctx, task)
    if not control_task.target_module or not control_task.target_function:
        await mark_control_run(
            control_task.run_id,
            status="failed",
            phase_detail="control target missing",
            progress_message="target missing",
        )
        raise RuntimeError("target_module and target_function are required")

    started_at_text = datetime.datetime.utcnow().isoformat()
    live_token = None
    heartbeat_task = None
    if control_task.run_id:
        live_token = set_live_progress_context(
            run_id=control_task.run_id,
            importer=control_task.importer,
            status="running",
            started_at=started_at_text,
            source="import-control-heartbeat",
            confidence="heartbeat",
        )
        heartbeat_task = asyncio.create_task(
            _live_progress_heartbeat(
                control_task.run_id,
                control_task.importer,
                control_task.target_function,
                started_at_text,
            )
        )

    await mark_control_run(
        control_task.run_id,
        status="running",
        phase_detail=f"{control_task.target_function} running",
        progress_message="running",
    )
    try:
        target_result = await _call_target_function(ctx, control_task)
        if control_task.call_style == "kwargs":
            await _mark_kwargs_success(control_task, target_result)
            return {"status": "succeeded", "run_id": control_task.run_id, "result": target_result}
    except Exception as exc:
        await mark_control_run(
            control_task.run_id,
            status="failed",
            phase_detail=f"{control_task.target_function} failed",
            progress_message="failed",
            error={"code": "import_failed", "message": str(exc)},
        )
        await _flush_terminal_status_events()
        raise
    finally:
        await _stop_live_progress_heartbeat(heartbeat_task)
        if live_token is not None:
            reset_live_progress_context(live_token)
    return {"status": "running", "run_id": control_task.run_id, "result": target_result}


def _control_task_from_payload(ctx: dict[str, Any], task: dict[str, Any] | None) -> ControlTask:
    request_payload_dict = task if isinstance(task, dict) else {}
    run_id = str(request_payload_dict.get("run_id") or "").strip()
    target_task_dict = request_payload_dict.get("task") if isinstance(request_payload_dict.get("task"), dict) else {}
    if run_id:
        target_task_dict = {**target_task_dict, "run_id": run_id}
        ctx["control_run_id"] = run_id
    return ControlTask(
        run_id=run_id,
        importer=str(request_payload_dict.get("importer") or request_payload_dict.get("target_function") or "unknown").strip(),
        target_module=str(request_payload_dict.get("target_module") or "").strip(),
        target_function=str(request_payload_dict.get("target_function") or "").strip(),
        call_style=str(request_payload_dict.get("call_style") or "ctx_task").strip(),
        target_task_dict=target_task_dict,
    )


async def _call_target_function(ctx: dict[str, Any], control_task: ControlTask) -> Any:
    module = import_module(control_task.target_module)
    target_function = getattr(module, control_task.target_function)
    accepted_params = signature(target_function).parameters
    if control_task.call_style == "kwargs":
        target_kwargs_dict = {
            key: target_value
            for key, target_value in control_task.target_task_dict.items()
            if key in accepted_params
        }
        return await target_function(**target_kwargs_dict)
    if len(accepted_params) >= 2:
        return await target_function(ctx, control_task.target_task_dict)
    return await target_function(ctx)


async def _mark_kwargs_success(control_task: ControlTask, target_result: Any) -> None:
    terminal_progress = _terminal_progress_from_result(control_task.target_function, target_result)
    terminal_metrics = target_result if isinstance(target_result, dict) else None
    await mark_control_run(
        control_task.run_id,
        status="succeeded",
        phase_detail=f"{control_task.target_function} succeeded",
        progress_message="succeeded",
        metrics=terminal_metrics,
        progress=terminal_progress,
    )
    await _flush_terminal_status_events()


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
    """Persist a control-run state transition and publish progress events."""
    if not run_id:
        return
    await ensure_import_run_table()
    schema = os.getenv("DB_SCHEMA") or "rx_data"
    progress_payload_dict = progress or _default_progress_payload(status, progress_message)
    transition = _run_transition_from_status(
        run_id=run_id,
        status=status,
        phase_detail=phase_detail,
        progress_message=progress_message,
        metrics=metrics,
        error=error,
    )
    cancel_guard = _cancel_guard_for_status(status)
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
        started_at=transition.started_at,
        heartbeat_at=transition.now,
        finished_at=transition.finished_at,
        progress=json.dumps(progress_payload_dict),
        metrics=None if metrics is None else json.dumps(metrics),
        error=None if error is None else json.dumps(error),
    )
    enqueue_live_progress(
        **_live_progress_payload(progress_payload_dict, transition)
    )
    enqueue_status_event(_status_event_payload(progress_payload_dict, transition))


def _run_transition_from_status(
    *,
    run_id: str,
    status: str,
    phase_detail: str,
    progress_message: str,
    metrics: dict[str, Any] | None,
    error: dict[str, Any] | None,
) -> ControlRunTransition:
    now = datetime.datetime.utcnow()
    finished_at = now if status in {"succeeded", "failed", "canceled", "dead_letter"} else None
    started_at = now if status == "running" else None
    return ControlRunTransition(
        run_id=run_id,
        status=status,
        phase_detail=phase_detail,
        progress_message=progress_message,
        metrics=metrics,
        error=error,
        now=now,
        started_at=started_at,
        finished_at=finished_at,
    )


def _cancel_guard_for_status(status: str) -> str:
    if status in {"running", "succeeded", "failed", "dead_letter"}:
        return "AND status NOT IN ('canceling', 'canceled')"
    return ""


def _default_progress_payload(status: str, progress_message: str) -> dict[str, Any]:
    is_terminal = status in {"succeeded", "failed", "canceled", "dead_letter"}
    return {"unit": "run", "total": 1, "done": 1 if is_terminal else 0, "pct": 100 if is_terminal else 0, "message": progress_message}


def _live_progress_payload(
    progress_payload_dict: dict[str, Any],
    transition: ControlRunTransition,
) -> dict[str, Any]:
    return {
        **progress_payload_dict,
        "run_id": transition.run_id,
        "status": transition.status,
        "phase": progress_payload_dict.get("phase") or transition.phase_detail,
        "message": progress_payload_dict.get("message") or transition.progress_message,
        "started_at": transition.started_at.isoformat() if transition.started_at else None,
        "finished_at": transition.finished_at.isoformat() if transition.finished_at else None,
        "publish_event": False,
    }


def _status_event_payload(
    progress_payload_dict: dict[str, Any],
    transition: ControlRunTransition,
) -> dict[str, Any]:
    return {
        "run_id": transition.run_id,
        "status": transition.status,
        "phase_detail": transition.phase_detail,
        "progress": progress_payload_dict,
        "metrics": transition.metrics or {},
        "error": transition.error,
        "heartbeat_at": transition.now.isoformat(),
        "started_at": transition.started_at.isoformat() if transition.started_at else None,
        "finished_at": transition.finished_at.isoformat() if transition.finished_at else None,
    }


async def ensure_import_run_table() -> None:
    """Create the import-run table used by controlled import workers."""
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
