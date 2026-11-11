"""Portable publication branches complement the native transaction proofs."""

import asyncio
import hashlib
import importlib
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_workers

publication = importlib.import_module("process.label_publish")


def _generation_state(oid=21, generation=3):
    return {
        "table_oid": oid,
        "generation": {
            "importer_id": "label",
            "local_lineage_id": "85f72b58-439e-4ada-b907-4933b18f8aa1",
            "local_generation": generation,
            "serving_generation": {
                "origin_lineage_id": "85f72b58-439e-4ada-b907-4933b18f8aa1",
                "origin_generation": generation,
                "published_at": "2026-09-21T00:00:00Z",
            },
            "relation_oids": [oid],
            "consumed_dependencies": {},
        },
    }


def _publication_receipt(run_id="example-run"):
    return {
        "format": "label-publication-v1",
        "run_id": run_id,
        "attempt_id": "example",
        "schema": "sample",
        "table_oid": 21,
        "node_id": "example-node",
        "source_label_count": 3,
        "imported_label_count": 3,
        "generation": _generation_state()["generation"],
        "published": True,
    }


def _publication_intent():
    return {
        "format": "label-publication-intent-v1",
        "run_id": "example-run",
        "attempt_id": "example",
        "schema": "sample",
        "stage_oid": 21,
        "node_id": "example-node",
        "source_label_count": 3,
    }


@asynccontextmanager
async def _transaction(events):
    events.append("begin")
    try:
        yield
    except BaseException:
        events.append("rollback")
        raise
    else:
        events.append("commit")


class _FreshSession:
    def __init__(self, events):
        self.events = events

    async def __aenter__(self):
        self.events.append("fresh-session")
        return self

    async def __aexit__(self, *_error):
        self.events.append("close-session")

    def begin(self):
        return _transaction(self.events)


@pytest.fixture
def publication_case(monkeypatch):
    receipt = _publication_receipt()
    events = []
    terminal_dict = {
        "status": "succeeded",
        "metrics": {"label_publication": receipt},
        "phase_detail": "label import published",
        "error": None,
        "finished_at": datetime.now(UTC),
        "heartbeat_at": datetime.now(UTC),
    }
    fresh = SimpleNamespace(
        status=AsyncMock(),
        first=AsyncMock(side_effect=[{"importer_id": "label"}, terminal_dict]),
        scalar=AsyncMock(return_value=21),
    )
    session = _FreshSession(events)
    database = SimpleNamespace(
        transaction=lambda: _transaction(events),
        status=AsyncMock(return_value=1),
        scalar=AsyncMock(side_effect=[21, 3]),
        first=AsyncMock(),
        session_factory=Mock(return_value=session),
    )
    context_dict = {
        "control_run_id": "example-run",
        "import_date": "example",
        "label_stage_oid": 21,
        "context": {"label_count": 3},
        "label_publication_preimage": _generation_state(11, 2),
    }
    monkeypatch.setenv("DB_SCHEMA", "sample")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "example-node")
    monkeypatch.setattr(publication, "_current_session_or_none", lambda: None)
    monkeypatch.setattr(publication, "ConnectionProxy", Mock(return_value=fresh))
    return SimpleNamespace(
        receipt=receipt,
        events=events,
        terminal=terminal_dict,
        fresh=fresh,
        session=session,
        database=database,
        context=context_dict,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("bound_oids", [None, (21,), (99,)])
async def test_state_binds_current_heap(monkeypatch, bound_oids):
    generation = SimpleNamespace(relation_oids=bound_oids, as_dict=lambda: {"local_generation": 3})
    monkeypatch.setattr(publication, "read_result_publication_authority", AsyncMock(return_value=generation))
    database = SimpleNamespace(scalar=AsyncMock(return_value=21))
    if bound_oids == (99,):
        with pytest.raises(RuntimeError, match="canonical heap differ"):
            await publication._label_state(database, "sample")
    else:
        assert await publication._label_state(database, "sample") == {
            "table_oid": 21,
            "generation": {"local_generation": 3},
        }
    assert database.scalar.call_args.kwargs == {"name": "sample.label"}


@pytest.mark.parametrize("session", [None, SimpleNamespace(in_transaction=lambda: False)])
def test_completion_requires_session_ownership(monkeypatch, session):
    monkeypatch.setattr(publication, "_current_session_or_none", lambda: session)
    if session is None:
        publication.require_label_completion_session()
    else:
        with pytest.raises(RuntimeError, match="top-level transaction"):
            publication.require_label_completion_session()


@pytest.mark.asyncio
async def test_success_cas_binds_receipt(publication_case, monkeypatch):
    case = publication_case
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "later-node")
    await publication._record_label_success(case.database, "sample", case.receipt, _publication_intent())
    (statement,) = case.database.status.call_args.args
    params = case.database.status.call_args.kwargs
    assert "status='running'" in str(statement) and "importer='label'" in str(statement)
    assert params["run_id"] == "example-run" and params["node_id"] == "example-node"
    assert json.loads(params["metrics"])["label_publication"] == case.receipt
    assert json.loads(params["progress"])["done"] == json.loads(params["progress"])["total"] == 3
    assert json.loads(params["intent"]) == _publication_intent()
    assert "metrics->'label_publication_intent'=CAST(:intent AS jsonb)" in str(statement)
    case.database.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_standalone_skips_managed_cas(publication_case):
    case = publication_case
    await publication._record_label_success(case.database, "sample", _publication_receipt(None), _publication_intent())
    case.database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, "running", "failed", "canceling", "canceled"])
async def test_success_refuses_lost_cas(publication_case, status):
    case = publication_case
    case.database.status.return_value = 0
    case.database.first.return_value = None if status is None else {"status": status}
    expected = asyncio.CancelledError if status in {"canceling", "canceled"} else RuntimeError
    with pytest.raises(expected):
        await publication._record_label_success(case.database, "sample", case.receipt, _publication_intent())
    assert "FOR UPDATE" in str(case.database.first.call_args.args[0])


def _mock_publication(monkeypatch, case, *, postimage=None):
    states = [case.context["label_publication_preimage"], postimage or _generation_state()]
    monkeypatch.setattr(publication, "read_result_publication_authority", AsyncMock())
    monkeypatch.setattr(publication, "_label_state", AsyncMock(side_effect=states))
    monkeypatch.setattr(publication, "publish_label_table", AsyncMock())
    monkeypatch.setattr(publication, "_record_label_success", AsyncMock())


@pytest.mark.asyncio
async def test_commit_records_atomic_candidate(publication_case, monkeypatch):
    case = publication_case
    _mock_publication(monkeypatch, case)
    receipt = await publication._commit_label_publication(case.database, case.context, "sample", "example")
    assert receipt == case.receipt == case.context["label_publication_candidate"]
    assert case.events == ["begin", "commit"]
    assert "ACCESS EXCLUSIVE" in str(case.database.status.call_args_list[1].args[0])
    publication.read_result_publication_authority.assert_awaited_once_with(
        case.database, importer_id="label", schema="sample", lock=True
    )
    publication._record_label_success.assert_awaited_once_with(case.database, "sample", receipt, _publication_intent())


@pytest.mark.asyncio
@pytest.mark.parametrize("stage_oid", [None, True, 99])
async def test_commit_rejects_changed_stage(publication_case, monkeypatch, stage_oid):
    case = publication_case
    _mock_publication(monkeypatch, case)
    case.database.scalar.side_effect = [stage_oid]
    with pytest.raises(RuntimeError, match="stage identity changed"):
        await publication._commit_label_publication(case.database, case.context, "sample", "example")
    publication.publish_label_table.assert_not_awaited()
    assert case.events[-1] == "rollback" and "label_publication_candidate" not in case.context


@pytest.mark.asyncio
@pytest.mark.parametrize("expected,count", [(0, 0), (True, 1), (3, 2)])
async def test_commit_rejects_incomplete_census(publication_case, monkeypatch, expected, count):
    case = publication_case
    _mock_publication(monkeypatch, case)
    case.context["context"]["label_count"] = expected
    case.database.scalar.side_effect = [21, count]
    with pytest.raises(ValueError, match="complete source count"):
        await publication._commit_label_publication(case.database, case.context, "sample", "example")
    publication.publish_label_table.assert_not_awaited()
    assert case.events[-1] == "rollback"


@pytest.mark.asyncio
@pytest.mark.parametrize("postimage", [_generation_state(99, 3), _generation_state(21, 4)])
async def test_commit_rejects_wrong_postimage(publication_case, monkeypatch, postimage):
    case = publication_case
    _mock_publication(monkeypatch, case, postimage=postimage)
    with pytest.raises(RuntimeError, match="verified stage"):
        await publication._commit_label_publication(case.database, case.context, "sample", "example")
    publication._record_label_success.assert_not_awaited()
    assert case.events[-1] == "rollback"


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_reconcile_confirms_exact_success(publication_case, monkeypatch, managed):
    case = publication_case
    receipt = case.receipt if managed else _publication_receipt(None)
    monkeypatch.setattr(publication, "_label_state", AsyncMock(return_value=_generation_state()))
    assert (
        await publication._reconcile_label_publication(case.database, case.context, "sample", "example", receipt)
        == "succeeded"
    )
    assert case.events == ["fresh-session", "begin", "commit", "close-session"]
    assert "FOR SHARE" in str(case.fresh.first.call_args.args[0])
    case.database.first.assert_not_awaited()
    case.database.session_factory.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, "running", "canceling", "canceled"])
async def test_reconcile_identifies_retained_stage(publication_case, monkeypatch, status):
    case = publication_case
    receipt = case.receipt if status is not None else _publication_receipt(None)
    case.terminal.update(status=status, metrics={})
    monkeypatch.setattr(publication, "_label_state", AsyncMock(return_value=case.context["label_publication_preimage"]))
    observed = await publication._reconcile_label_publication(case.database, case.context, "sample", "example", receipt)
    assert observed == ("canceled" if status in {"canceling", "canceled"} else "running")
    assert case.fresh.scalar.call_args.kwargs == {"name": "sample.label_example"}


@pytest.mark.asyncio
async def test_reconcile_requires_managed_row(publication_case):
    case = publication_case
    case.fresh.first.side_effect = [{"importer_id": "label"}, None]
    with pytest.raises(RuntimeError, match="run is unavailable"):
        await publication._reconcile_label_publication(case.database, case.context, "sample", "example", case.receipt)
    assert case.events[-2:] == ["rollback", "close-session"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["receipt", "phase", "error", "finished", "heartbeat", "stage", "state"])
async def test_reconcile_refuses_damaged_evidence(publication_case, monkeypatch, damage):
    case = publication_case
    postimage = _generation_state()
    mutations_by_damage = {
        "receipt": ("metrics", {}),
        "phase": ("phase_detail", "running"),
        "error": ("error", {}),
        "finished": ("finished_at", None),
        "heartbeat": ("heartbeat_at", None),
    }
    if damage in mutations_by_damage:
        key, value = mutations_by_damage[damage]
        case.terminal[key] = value
    if damage == "stage":
        postimage = case.context["label_publication_preimage"]
        case.terminal.update(status="running", metrics={})
        case.fresh.scalar.return_value = 99
    if damage == "state":
        postimage = _generation_state(99, 3)
    monkeypatch.setattr(publication, "_label_state", AsyncMock(return_value=postimage))
    with pytest.raises(RuntimeError, match="outcome could not be confirmed"):
        await publication._reconcile_label_publication(case.database, case.context, "sample", "example", case.receipt)


@pytest.mark.asyncio
async def test_reconcile_drains_parent_cancellation(publication_case, monkeypatch):
    case = publication_case
    started, release = asyncio.Event(), asyncio.Event()

    async def delayed(*_arguments):
        started.set()
        await release.wait()
        return "succeeded"

    monkeypatch.setattr(publication, "_reconcile_label_publication", delayed)
    worker = asyncio.create_task(
        publication._await_label_reconciliation(case.database, case.context, "sample", "example", case.receipt)
    )
    try:
        await started.wait()
        worker.cancel()
        await asyncio.sleep(0)
        assert not worker.done()
        release.set()
        assert await worker == "succeeded"
    finally:
        release.set()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_reconcile_propagates_child_cancellation(publication_case, monkeypatch):
    case = publication_case
    monkeypatch.setattr(publication, "_reconcile_label_publication", AsyncMock(side_effect=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await publication._await_label_reconciliation(case.database, case.context, "sample", "example", case.receipt)


@pytest.mark.asyncio
async def test_completion_returns_committed_result(publication_case, monkeypatch):
    case = publication_case
    monkeypatch.setattr(publication, "_commit_label_publication", AsyncMock(return_value=case.receipt))
    assert await publication.complete_label_publication(case.database, case.context) == case.receipt
    assert "label_commit_unknown" not in case.context


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["succeeded", "running", "canceled", "unavailable", "unprepared"])
async def test_completion_classifies_commit_failure(publication_case, monkeypatch, status):
    case = publication_case

    async def uncertain(*_arguments):
        if status != "unprepared":
            case.context["label_publication_candidate"] = case.receipt
        raise RuntimeError("commit response lost")

    reconcile = AsyncMock(return_value=status)
    if status == "unavailable":
        reconcile.side_effect = TimeoutError("reconciliation unavailable")
    monkeypatch.setattr(publication, "_commit_label_publication", uncertain)
    monkeypatch.setattr(publication, "_await_label_reconciliation", reconcile)
    if status == "succeeded":
        assert await publication.complete_label_publication(case.database, case.context) == case.receipt
    else:
        errors_by_status = {"canceled": asyncio.CancelledError, "unavailable": TimeoutError}
        with pytest.raises(errors_by_status.get(status, RuntimeError)):
            await publication.complete_label_publication(case.database, case.context)
    assert bool(case.context.get("label_commit_unknown")) == (status == "unavailable")
    if status == "unprepared":
        reconcile.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_completion_retains_cancellation(publication_case, monkeypatch, committed):
    case = publication_case

    async def canceled_commit(*_arguments):
        case.context["label_publication_candidate"] = case.receipt
        asyncio.current_task().cancel()
        await asyncio.sleep(0)

    monkeypatch.setattr(publication, "_commit_label_publication", canceled_commit)
    monkeypatch.setattr(
        publication, "_await_label_reconciliation", AsyncMock(return_value="succeeded" if committed else "running")
    )
    worker = asyncio.create_task(publication.complete_label_publication(case.database, case.context))
    if committed:
        assert await worker == case.receipt
        assert worker.cancelling() == 0
    else:
        with pytest.raises(asyncio.CancelledError):
            await worker
    assert "label_commit_unknown" not in case.context


@pytest.mark.parametrize("managed", [False, True])
def test_intent_binds_stage_identity(publication_case, monkeypatch, managed):
    case = publication_case
    if not managed:
        case.context.pop("control_run_id")
        monkeypatch.delenv("HLTHPRT_IMPORT_NODE_ID")
    intent_dict = _publication_intent()
    if not managed:
        intent_dict.update(run_id=None, node_id=None)
    assert publication.label_publication_intent(case.context, "sample") == intent_dict


def _recovery_row(case, status="succeeded"):
    suffix = "l" + hashlib.sha256(case.receipt["run_id"].encode()).hexdigest()[:32]
    intent_dict = {**_publication_intent(), "attempt_id": suffix}
    receipt_dict = {**case.receipt, "attempt_id": suffix}
    return {
        **case.terminal,
        "run_id": "example-run",
        "node_id": "example-node",
        "status": status,
        "metrics": {
            "label_publication_intent": intent_dict,
            **({"label_publication": receipt_dict} if status == "succeeded" else {}),
        },
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("format", "unknown"),
        ("run_id", "other-run"),
        ("attempt_id", "other-attempt"),
        ("schema", "other_schema"),
        ("node_id", "other-node"),
        ("stage_oid", True),
        ("stage_oid", 0),
        ("source_label_count", True),
        ("source_label_count", 0),
        ("extra", "unexpected"),
    ],
)
def test_recovery_refuses_foreign_intent(publication_case, field, value):
    row = _recovery_row(publication_case)
    row["metrics"]["label_publication_intent"][field] = value
    with pytest.raises(RuntimeError, match="exact retained attempt identity"):
        publication._retained_label_intent(row, "sample")


@pytest.mark.parametrize("metrics", [None, {}, {"label_publication_intent": []}])
def test_recovery_requires_persisted_intent(publication_case, metrics):
    row = _recovery_row(publication_case)
    row["metrics"] = metrics
    with pytest.raises(RuntimeError, match="exact retained attempt identity"):
        publication._retained_label_intent(row, "sample")


@pytest.mark.parametrize("damage", ["receipt", "phase", "error", "finished", "heartbeat", "generation"])
def test_recovery_refuses_damaged_completion(publication_case, damage):
    row = _recovery_row(publication_case)
    intent = row["metrics"]["label_publication_intent"]
    mutations_by_damage = {
        "receipt": ("metrics", {"label_publication_intent": intent}),
        "phase": ("phase_detail", "running"),
        "error": ("error", {}),
        "finished": ("finished_at", None),
        "heartbeat": ("heartbeat_at", None),
    }
    if damage == "generation":
        row["metrics"]["label_publication"]["generation"] = _generation_state(99)["generation"]
    else:
        field, value = mutations_by_damage[damage]
        row[field] = value
    with pytest.raises(RuntimeError, match="recovery (completion|generation) receipt differs"):
        publication._confirmed_label_recovery(row, intent)


def _mock_recovery(monkeypatch, case, row):
    monkeypatch.setattr(publication, "require_label_ordinary_publication", AsyncMock())
    monkeypatch.setattr(publication, "read_result_publication_authority", AsyncMock())
    case.database.first.return_value = row
    case.database.scalar.side_effect = [21, 11]


@pytest.mark.asyncio
async def test_recovery_accepts_historical_success(publication_case, monkeypatch):
    case = publication_case
    row = _recovery_row(case)
    _mock_recovery(monkeypatch, case, row)
    publication.read_result_publication_authority.return_value = _generation_state(99, 9)
    result = await publication.reconcile_managed_label_run(case.database, "example-run")
    assert result == {
        "run_id": "example-run",
        "stage": "sample.label_" + row["metrics"]["label_publication_intent"]["attempt_id"],
        "stage_oid": 21,
        "status": "succeeded",
        "publication": "committed",
    }
    assert case.events == ["begin", "commit"]
    case.database.scalar.assert_not_awaited()
    assert case.database.status.await_count == 2
    assert "FOR UPDATE" in str(case.database.first.call_args.args[0])
    publication.read_result_publication_authority.assert_awaited_once_with(
        case.database, importer_id="label", schema="sample", lock=True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "finalizing", "canceling", "failed", "canceled"])
async def test_recovery_preserves_unpublished_stage(publication_case, monkeypatch, status):
    case = publication_case
    row = _recovery_row(case, status)
    _mock_recovery(monkeypatch, case, row)
    result = await publication.reconcile_managed_label_run(case.database, "example-run")
    assert result["status"] == ("canceled" if status in {"canceling", "canceled"} else "failed")
    assert result["stage_oid"] == 21 and result["publication"] == "unpublished"
    assert [call.kwargs["name"] for call in case.database.scalar.call_args_list] == [result["stage"], "sample.label"]
    statements = [str(call.args[0]) for call in case.database.status.call_args_list]
    assert not any("DROP" in statement or "DELETE" in statement for statement in statements)
    assert case.events == ["begin", "commit"]
    if status in {"failed", "canceled"}:
        assert case.database.status.await_count == 2
    else:
        params = case.database.status.call_args.kwargs
        assert params["previous"] == status and params["status"] == result["status"]
        assert json.loads(params["identity"]) == row["metrics"]["label_publication_intent"]
        assert params["identity_field"] == "label_publication_intent"
        assert json.loads(params["error"])["code"] == "label_publication_uncommitted"


@pytest.mark.asyncio
@pytest.mark.parametrize("run_id", [None, True, ""])
async def test_recovery_requires_one_run(publication_case, run_id):
    case = publication_case
    with pytest.raises(ValueError, match="one managed run ID"):
        await publication.reconcile_managed_label_run(case.database, run_id)
    assert case.events == []
    case.database.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_recovery_refuses_foreign_run(publication_case, monkeypatch, missing):
    case = publication_case
    row = None if missing else {**_recovery_row(case), "node_id": "other-node"}
    _mock_recovery(monkeypatch, case, row)
    with pytest.raises(RuntimeError, match="run identity differs"):
        await publication.reconcile_managed_label_run(case.database, "example-run")
    case.database.scalar.assert_not_awaited()
    assert case.events == ["begin", "rollback"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["status", "receipt", "missing-stage", "foreign-stage", "canonical"])
async def test_recovery_refuses_ambiguous_state(publication_case, monkeypatch, damage):
    case = publication_case
    row = _recovery_row(case, "running")
    _mock_recovery(monkeypatch, case, row)
    if damage == "status":
        row["status"] = "queued"
    elif damage == "receipt":
        row["metrics"]["label_publication"] = case.receipt
    elif damage == "missing-stage":
        case.database.scalar.side_effect = [None, 11]
    elif damage == "foreign-stage":
        case.database.scalar.side_effect = [99, 11]
    else:
        case.database.scalar.side_effect = [21, 21]
    with pytest.raises(RuntimeError, match="outcome remains unconfirmed"):
        await publication.reconcile_managed_label_run(case.database, "example-run")
    assert case.database.status.await_count == 2
    assert case.events == ["begin", "rollback"]


@pytest.mark.asyncio
async def test_recovery_rolls_back_failed_cas(publication_case, monkeypatch):
    case = publication_case
    _mock_recovery(monkeypatch, case, _recovery_row(case, "running"))
    case.database.status.side_effect = [None, None, 0]
    with pytest.raises(RuntimeError, match="recovery run changed"):
        await publication.reconcile_managed_label_run(case.database, "example-run")
    assert case.events == ["begin", "rollback"]


@pytest.mark.asyncio
async def test_recovery_requires_unbound_session(publication_case, monkeypatch):
    case = publication_case
    monkeypatch.setattr(publication, "_current_session_or_none", lambda: SimpleNamespace())
    with pytest.raises(RuntimeError, match="top-level transaction"):
        await publication.reconcile_managed_label_run(case.database, "example-run")
    assert case.events == []
    case.database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "succeeded"])
async def test_recovery_accepts_optional_node(publication_case, monkeypatch, status):
    case = publication_case
    row = _recovery_row(case, status)
    row["metrics"]["label_publication_intent"]["node_id"] = None
    if status == "succeeded":
        row["metrics"]["label_publication"]["node_id"] = None
    monkeypatch.delenv("HLTHPRT_IMPORT_NODE_ID")
    _mock_recovery(monkeypatch, case, row)
    result = await publication.reconcile_managed_label_run(case.database, "example-run")
    assert result["status"] == ("failed" if status == "running" else "succeeded")
    assert row["node_id"] == "example-node"


def _mock_pre_stage(monkeypatch, case, status="running"):
    run_row_dict = _recovery_row(case, status)
    run_row_dict["metrics"] = {
        "label_publication_protocol": publication.label_publication_protocol("example-run", "sample", "example-node"),
        "label_worker_claim": {
            "format": "label-worker-claim-v1",
            "run_id": "example-run",
            "claim_id": "example-claim",
            "arq_job_id": "example-dequeue",
            "job_name": "actual-worker",
            "job_uid": "example-uid",
        },
    }
    _mock_recovery(monkeypatch, case, run_row_dict)
    case.database.scalar.side_effect = None
    case.database.scalar.return_value = None
    image = "ghcr.io/example/worker@sha256:" + "1" * 64
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", image)
    state_dict = {
        "launcher": "kubernetes",
        "run_id": "other-launch-run",
        "job_name": "actual-worker",
        "terminal_status": "failed",
        "active_jobs": 0,
        "running": False,
        "job_uid": "example-uid",
        "worker_image": image,
    }
    worker = Mock(return_value=state_dict)
    monkeypatch.setattr(control_workers, "_kubernetes_worker_state", worker)
    return run_row_dict, worker


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["queued", "starting", "running", "finalizing", "canceling", "failed", "canceled"])
async def test_recovery_closes_proven_pre_stage(publication_case, monkeypatch, status):
    case = publication_case
    row, worker = _mock_pre_stage(monkeypatch, case, status)
    result = await publication.reconcile_managed_label_run(case.database, "example-run")
    assert result["status"] == ("canceled" if status in {"canceling", "canceled"} else "failed")
    assert result["stage_oid"] is None and result["publication"] == "unpublished"
    worker.assert_called_once_with(control_workers._BY_IMPORTER["label"], claimed_job_name="actual-worker")
    if status not in {"failed", "canceled"}:
        params = case.database.status.call_args.kwargs
        assert params["identity_field"] == "label_publication_protocol"
        assert json.loads(params["identity"]) == row["metrics"]["label_publication_protocol"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("launcher", "process"),
        ("job_name", "other"),
        ("terminal_status", None),
        ("active_jobs", 1),
        ("running", True),
        ("job_uid", None),
        ("worker_image", "ghcr.io/example/old:tag"),
    ],
)
async def test_pre_stage_requires_exact_worker(publication_case, monkeypatch, field, value):
    case = publication_case
    _, worker = _mock_pre_stage(monkeypatch, case)
    worker.return_value[field] = value
    with pytest.raises(RuntimeError, match="exact terminal worker evidence"):
        await publication.reconcile_managed_label_run(case.database, "example-run")
    assert case.events == ["begin", "rollback"]
    assert case.database.status.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["protocol", "receipt", "process", "stage", "status", "image"])
async def test_pre_stage_preserves_ambiguous_evidence(publication_case, monkeypatch, damage):
    case = publication_case
    row, worker = _mock_pre_stage(monkeypatch, case)
    if damage == "protocol":
        row["metrics"]["label_publication_protocol"]["attempt_id"] = "foreign"
    if damage == "receipt":
        row["metrics"]["label_publication"] = case.receipt
    if damage == "process":
        monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "process")
    if damage == "stage":
        case.database.scalar.return_value = 21
    if damage == "status":
        row["status"] = "succeeded"
    if damage == "image":
        monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/example/worker:tag")
    with pytest.raises(RuntimeError):
        await publication.reconcile_managed_label_run(case.database, "example-run")
    assert case.events == ["begin", "rollback"]
    assert case.database.status.await_count == 2
    if damage in {"protocol", "receipt", "process"}:
        worker.assert_not_called()


@pytest.mark.asyncio
async def test_claim_binds_actual_dequeue_atomically(publication_case, monkeypatch):
    case = publication_case
    context_dict = {"job_id": "dequeued-message"}
    monkeypatch.setenv("HLTHPRT_LABEL_WORKER_JOB_NAME", "actual-worker")
    monkeypatch.setenv("HLTHPRT_LABEL_WORKER_JOB_UID", "actual-uid")
    image = "ghcr.io/example/worker@sha256:" + "1" * 64
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", image)
    await publication.claim_label_run(case.database, context_dict, "example-run")
    first = case.database.status.call_args
    await publication.claim_label_run(case.database, context_dict, "example-run")
    assert case.database.status.call_args.kwargs == first.kwargs
    assert str(case.database.status.call_args.args[0]) == str(first.args[0])
    claim_dict = json.loads(first.kwargs["claim"])
    assert claim_dict == {
        "format": "label-worker-claim-v2",
        "run_id": "example-run",
        "claim_id": context_dict["label_worker_claim_id"],
        "arq_job_id": "dequeued-message",
        "job_name": "actual-worker",
        "job_uid": "actual-uid",
        "worker_image": image,
    }
    assert json.loads(first.kwargs["metrics"]) == {"label_worker_claim": claim_dict}
    statement = str(first.args[0])
    assert "status IN ('queued','starting','running','finalizing')" in statement
    assert "status='running' AND metrics->'label_worker_claim'=CAST(:claim AS jsonb)" in statement
    assert "label_publication_intent" in statement and "node_id=:node_id" in statement


@pytest.mark.asyncio
async def test_claim_refuses_lost_ownership(publication_case):
    case = publication_case
    case.database.status.return_value = 0
    with pytest.raises(RuntimeError, match="active run ownership fence"):
        await publication.claim_label_run(case.database, {"job_id": "other-message"}, "example-run")
    case.database.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_claim_honors_owned_cancellation(publication_case):
    case = publication_case
    case.database.status.side_effect = [0, 1]
    with pytest.raises(asyncio.CancelledError, match="canceled before acquisition"):
        await publication.claim_label_run(case.database, {"job_id": "owned-message"}, "example-run")
    statement = str(case.database.status.call_args.args[0])
    assert "status='canceling'" in statement and "metrics->'label_worker_claim'=CAST(:claim AS jsonb)" in statement


@pytest.mark.asyncio
async def test_standalone_skips_managed_claim(publication_case):
    context_dict = {}
    await publication.claim_label_run(publication_case.database, context_dict, None)
    publication_case.database.status.assert_not_awaited()
    assert context_dict == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "format", "run_id", "claim_id", "arq_job_id", "job_name", "job_uid"])
async def test_pre_stage_requires_persisted_consumer(publication_case, monkeypatch, damage):
    case = publication_case
    row, worker = _mock_pre_stage(monkeypatch, case)
    if damage == "missing":
        row["metrics"].pop("label_worker_claim")
    else:
        row["metrics"]["label_worker_claim"][damage] = "foreign" if damage in {"format", "run_id"} else None
    with pytest.raises(RuntimeError, match="exact worker claim"):
        await publication.reconcile_managed_label_run(case.database, "example-run")
    worker.assert_not_called()
    assert case.database.status.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("current_image", [None, "changed", "tag"])
async def test_recovery_uses_original_claim_image(publication_case, monkeypatch, current_image):
    case = publication_case
    row, worker = _mock_pre_stage(monkeypatch, case)
    claim_dict = row["metrics"]["label_worker_claim"]
    claim_dict.update(format="label-worker-claim-v2", worker_image=worker.return_value["worker_image"])
    if current_image is None:
        monkeypatch.delenv("HLTHPRT_WORKER_JOB_IMAGE")
    else:
        monkeypatch.setenv(
            "HLTHPRT_WORKER_JOB_IMAGE",
            "ghcr.io/example/worker@sha256:" + "2" * 64 if current_image == "changed" else "example:tag",
        )
    recovered_dict = await publication.reconcile_managed_label_run(case.database, "example-run")
    assert recovered_dict["status"] == "failed"
    assert claim_dict["worker_image"] == worker.return_value["worker_image"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "extra", "tag", "null", "foreign-job"])
async def test_recovery_refuses_damaged_image_claim(publication_case, monkeypatch, damage):
    case = publication_case
    row, worker = _mock_pre_stage(monkeypatch, case)
    claim_dict = row["metrics"]["label_worker_claim"]
    claim_dict.update(format="label-worker-claim-v2", worker_image=worker.return_value["worker_image"])
    if damage == "missing":
        claim_dict.pop("worker_image")
    elif damage == "extra":
        claim_dict["unrecognized"] = "value"
    elif damage == "foreign-job":
        worker.return_value["worker_image"] = "ghcr.io/example/worker@sha256:" + "2" * 64
    else:
        claim_dict["worker_image"] = "example:tag" if damage == "tag" else None
    with pytest.raises(RuntimeError):
        await publication.reconcile_managed_label_run(case.database, "example-run")
    assert case.events == ["begin", "rollback"]


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["image", "uid", "name", "tag"])
async def test_runtime_claim_requires_exact_image(publication_case, monkeypatch, missing):
    monkeypatch.setenv("HLTHPRT_LABEL_WORKER_JOB_NAME", "actual-worker")
    monkeypatch.setenv("HLTHPRT_LABEL_WORKER_JOB_UID", "actual-uid")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/example/worker@sha256:" + "1" * 64)
    if missing == "tag":
        monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "example:tag")
    else:
        key = {
            "image": "HLTHPRT_WORKER_JOB_IMAGE",
            "uid": "HLTHPRT_LABEL_WORKER_JOB_UID",
            "name": "HLTHPRT_LABEL_WORKER_JOB_NAME",
        }[missing]
        monkeypatch.delenv(key)
    context_dict = {"job_id": "message"}
    with pytest.raises(RuntimeError, match="exact immutable runtime image"):
        await publication.claim_label_run(publication_case.database, context_dict, "example-run")
    publication_case.database.status.assert_not_awaited()
    assert context_dict == {"job_id": "message"}
