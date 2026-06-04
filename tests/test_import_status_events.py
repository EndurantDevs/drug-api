import json

import pytest

from api import control_imports
from process import import_status_events
from process import live_progress


def test_status_event_noops_without_import_control_url(monkeypatch):
    monkeypatch.delenv("HLTHPRT_IMPORT_CONTROL_URL", raising=False)
    monkeypatch.delenv("HP_IMPORT_CONTROL_BASE_URL", raising=False)

    import_status_events.enqueue_status_event({"run_id": "run_1", "status": "running"})


def test_status_event_throttles_repeated_phase_but_allows_transition(monkeypatch):
    queued = []

    class FakeQueue:
        def __init__(self):
            self.items = []

        def full(self):
            return False

        def put_nowait(self, item):
            self.items.append(item)
            queued.append(item)

    fake_queue = FakeQueue()
    monkeypatch.setenv("HLTHPRT_IMPORT_CONTROL_URL", "http://import-control")
    monkeypatch.setattr(import_status_events, "_ensure_queue", lambda _loop: fake_queue)
    monkeypatch.setattr(import_status_events.asyncio, "get_running_loop", lambda: object())
    import_status_events._last_sent_by_run.clear()

    import_status_events.enqueue_status_event({"run_id": "run_1", "status": "running", "phase_detail": "download"})
    import_status_events.enqueue_status_event({"run_id": "run_1", "status": "running", "phase_detail": "download"})
    import_status_events.enqueue_status_event({"run_id": "run_1", "status": "running", "phase_detail": "publish"})

    assert [item["phase_detail"] for item in queued] == ["download", "publish"]


def test_live_progress_writes_redis_ttl_and_estimate(monkeypatch):
    writes = []
    events = []

    class FakeRedis:
        def setex(self, key, ttl, value):
            writes.append((key, ttl, value))

    monkeypatch.setattr(live_progress, "_redis", lambda: FakeRedis())
    monkeypatch.setattr(live_progress, "enqueue_status_event", events.append)

    live_progress.write_live_progress(
        run_id="run_ndc",
        importer="ndc",
        status="running",
        phase="ndc records",
        unit="records",
        done=5,
        total=10,
        elapsed_seconds=20,
        message="importing records",
    )

    assert writes[0][0] == "import:progress:run_ndc"
    assert writes[0][1] == live_progress.IMPORT_LIVE_PROGRESS_TTL_SECONDS
    payload = json.loads(writes[0][2])
    assert payload["pct"] == 50
    assert payload["eta_seconds"] == 20
    assert events[0]["importer"] == "ndc"
    assert events[0]["estimate"]["eta_seconds"] == 20


def test_live_progress_preserves_earliest_started_at(monkeypatch):
    writes = []
    previous = {
        "run_id": "run_ndc",
        "importer": "ndc",
        "status": "running",
        "started_at": "2026-06-03T12:00:00Z",
        "updated_at": "2026-06-03T12:01:00Z",
    }

    class FakeRedis:
        def get(self, key):
            assert key == "import:progress:run_ndc"
            return json.dumps(previous).encode("utf-8")

        def setex(self, key, ttl, value):
            writes.append((key, ttl, value))

    monkeypatch.setattr(live_progress, "_redis", lambda: FakeRedis())
    monkeypatch.setattr(live_progress, "enqueue_status_event", lambda _event: None)

    live_progress.write_live_progress(
        run_id="run_ndc",
        importer="ndc",
        status="running",
        started_at="2026-06-03T12:05:00Z",
        unit="records",
        done=5,
        total=10,
        message="parsing",
    )

    payload = json.loads(writes[0][2])
    assert payload["started_at"] == previous["started_at"]


def test_control_imports_overlay_live_progress_for_active_run(monkeypatch):
    monkeypatch.setattr(
        control_imports,
        "read_live_progress",
        lambda _run_id: {
            "phase": "ndc records",
            "pct": 35,
            "eta_seconds": 60,
            "estimated_finish_at": "2026-06-03T12:01:00Z",
            "updated_at": "2026-06-03T12:00:00Z",
            "message": "importing records",
        },
    )

    result = control_imports._overlay_live_progress(
        {"run_id": "run_ndc", "importer": "ndc", "status": "running", "progress": {"pct": 0}}
    )

    assert result["phase_detail"] == "ndc records"
    assert result["progress"]["pct"] == 35
    assert result["estimate"]["eta_seconds"] == 60


@pytest.mark.asyncio
async def test_request_cancel_rejects_running_non_cancelable_importer(monkeypatch):
    async def fake_get(_run_id):
        return {
            "run_id": "run_ndc",
            "importer": "ndc",
            "status": "running",
            "progress": {"pct": 25},
            "metrics": {},
        }

    monkeypatch.setattr(control_imports, "get_import_run", fake_get)

    with pytest.raises(ValueError, match="does not support canceling active runs"):
        await control_imports.request_cancel("run_ndc")
