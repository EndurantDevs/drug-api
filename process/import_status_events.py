from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any

ENGINE_NAME = "drug-api"
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
logger = logging.getLogger(__name__)


@dataclass
class _PublisherState:
    queue: asyncio.Queue[dict[str, Any]] | None = None
    worker: asyncio.Task | None = None
    last_sent_by_run: dict[str, tuple[float, str, str]] = field(default_factory=dict)


_publisher_state = _PublisherState()
_last_sent_by_run = _publisher_state.last_sent_by_run


def enqueue_status_event(run_payload: dict[str, Any]) -> None:
    """Queue a best-effort status event for import-control."""
    if not _import_control_url():
        return
    run_id = str(run_payload.get("run_id") or "").strip()
    if not run_id:
        return
    status = str(run_payload.get("status") or "").strip()
    phase = str(run_payload.get("phase_detail") or (run_payload.get("progress") or {}).get("phase") or "").strip()
    is_terminal = status in TERMINAL_STATUSES
    now = time.monotonic()
    if not is_terminal:
        throttle_seconds = _throttle_seconds()
        previous = _publisher_state.last_sent_by_run.get(run_id)
        if previous is not None and previous[1] == status and previous[2] == phase and now - previous[0] < throttle_seconds:
            return
        _publisher_state.last_sent_by_run[run_id] = (now, status, phase)
    event_dict = {"engine": ENGINE_NAME, "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"), **run_payload}
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    queue = _ensure_queue(loop)
    if queue.full():
        try:
            queue.get_nowait()
            queue.task_done()
        except asyncio.QueueEmpty:
            logger.debug("status event queue became empty before dropping the oldest event")
    try:
        queue.put_nowait(event_dict)
    except asyncio.QueueFull:
        return


async def flush_status_events(timeout_seconds: float = 2.0) -> None:
    """Wait for queued status events to be published, up to a timeout."""
    queue = _publisher_state.queue
    if queue is None:
        return
    try:
        await asyncio.wait_for(queue.join(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        return


def _ensure_queue(loop: asyncio.AbstractEventLoop) -> asyncio.Queue[dict[str, Any]]:
    if _publisher_state.queue is None:
        _publisher_state.queue = asyncio.Queue(maxsize=max(int(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_QUEUE_SIZE", "256")), 1))
    if _publisher_state.worker is None or _publisher_state.worker.done():
        _publisher_state.worker = loop.create_task(_publisher_worker(_publisher_state.queue))
    return _publisher_state.queue


async def _publisher_worker(queue: asyncio.Queue[dict[str, Any]]) -> None:
    while True:
        event = await queue.get()
        try:
            await asyncio.to_thread(_post_event, event)
        except Exception as exc:
            logger.debug(
                "failed to publish import status event run_id=%s status=%s: %s",
                event.get("run_id"),
                event.get("status"),
                exc,
            )
        finally:
            queue.task_done()


def _post_event(event: dict[str, Any]) -> None:
    base_url = _import_control_url()
    if not base_url:
        return
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/v1/runs/events",
        data=json.dumps(event, default=str).encode("utf-8"),
        method="POST",
        headers={"content-type": "application/json", **_auth_headers()},
    )
    with urllib.request.urlopen(request, timeout=_timeout_seconds()) as response:
        response.read()


def _auth_headers() -> dict[str, str]:
    token = str(os.getenv("HLTHPRT_IMPORT_CONTROL_TOKEN") or os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _import_control_url() -> str:
    return str(os.getenv("HLTHPRT_IMPORT_CONTROL_URL") or os.getenv("HP_IMPORT_CONTROL_BASE_URL") or "").strip()


def _timeout_seconds() -> float:
    return max(float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TIMEOUT_SECONDS", "1.0")), 0.1)


def _throttle_seconds() -> float:
    return max(float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_THROTTLE_SECONDS", "5.0")), 0.0)
