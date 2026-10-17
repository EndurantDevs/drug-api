"""A completed protected stage is evidence, never permission to publish."""

import asyncio
import importlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from click.testing import CliRunner

from api import control_imports
from process import label_stage

label = importlib.import_module("process.label")


@asynccontextmanager
async def transaction():
    yield SimpleNamespace(execute=AsyncMock(), connection=AsyncMock(return_value=SimpleNamespace(run_sync=AsyncMock())))


def database(*, owner="synthetic-run", oid=71, comment="owned", count=2, changed=1):
    return SimpleNamespace(
        transaction=transaction,
        status=AsyncMock(return_value=changed),
        scalar=AsyncMock(side_effect=[oid, comment, owner, count]),
        all=AsyncMock(
            return_value=[
                {"oid": n, "definition": "synthetic index", "valid": True, "ready": True, "primary": n == 0}
                for n in range(5)
            ]
        ),
    )


async def finish(db):
    return await label_stage.finish_label_stage(db, "synthetic", "synthetic-run", "l" + "a" * 32, 71, "owned", 2)


def protect_label_source(monkeypatch, total_records):
    monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", "https://example.test/manifest")
    monkeypatch.setattr(
        label_stage,
        "require_label_ordinary_publication",
        AsyncMock(side_effect=RuntimeError("ordinary Label publication is disabled for the protected live table")),
    )
    monkeypatch.setattr(label_stage, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(label_stage, "enqueue_status_event", lambda _event: None)
    manifest_dict = {
        "results": {
            "drug": {
                "label": {
                    "total_records": total_records,
                    "partitions": [{"file": "https://example.test/part.zip", "records": total_records}],
                }
            }
        }
    }
    monkeypatch.setattr(
        label_stage,
        "download_it",
        AsyncMock(return_value=SimpleNamespace(content=json.dumps(manifest_dict), raise_for_status=lambda: None)),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed,cleanup_fails", [(0, False), (1, False), (1, True)])
async def test_failed_attempt_notifies_only_after_owned_transition(monkeypatch, changed, cleanup_fails):
    database = SimpleNamespace(status=AsyncMock(return_value=changed))
    attempt = SimpleNamespace(
        schema="synthetic",
        run_id="synthetic-run",
        suffix="l" + "a" * 32,
        oid=71,
        owner_comment="owned",
    )
    cleanup = AsyncMock(side_effect=TimeoutError("synthetic timeout") if cleanup_fails else None)
    events = []
    live_events = []
    monkeypatch.setattr(label_stage, "has_discarded_failed_label_stage", cleanup)
    monkeypatch.setattr(label_stage, "enqueue_status_event", events.append)
    monkeypatch.setattr(label_stage, "enqueue_live_progress", lambda **fields: live_events.append(fields))

    await label_stage._reconcile_failed_label_stage(database, attempt)

    assert "phase_detail='label stage failed'" in str(database.status.await_args.args[0])
    assert json.loads(database.status.await_args.kwargs["progress"])["message"] == "failed"
    if changed:
        cleanup.assert_awaited_once()
        assert len(events) == len(live_events) == 1
        assert events[0]["status"] == live_events[0]["status"] == "failed"
        assert events[0]["error"] == {"code": "label_stage_failed"}
    else:
        cleanup.assert_not_awaited()
        assert events == live_events == []


@pytest.mark.asyncio
async def test_completed_stage_retains_exact_evidence_without_publication():
    db = database()
    receipt = await finish(db)
    assert receipt["run_id"] == "synthetic-run"
    assert receipt["attempt_id"] == "l" + "a" * 32
    assert receipt["relation_oid"] == 71
    assert receipt["row_count"] == receipt["source_row_count"] == 2
    assert len(receipt["indexes"]) == 5
    statements = [str(call.args[0]) for call in db.status.await_args_list]
    assert "status='finalizing'" in statements[-1]
    assert all("RENAME" not in sql and "DROP" not in sql and "succeeded" not in sql for sql in statements)
    assert db.status.await_args_list[-1].kwargs["attempt_id"] == receipt["attempt_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides,match",
    [
        ({"oid": 72}, "identity changed"),
        ({"comment": "foreign"}, "identity changed"),
        ({"owner": None}, "ownership or status"),
        ({"count": 1}, "census"),
        ({"changed": 0}, "ownership fence"),
    ],
)
async def test_stale_duplicate_canceled_and_failed_finalization_rejected(overrides, match):
    with pytest.raises(RuntimeError, match=match):
        await finish(database(**overrides))


@pytest.mark.asyncio
async def test_invalid_indexes_cannot_finalize():
    db = database()
    db.all.return_value[0]["valid"] = False
    with pytest.raises(RuntimeError, match="index validation"):
        await finish(db)
    assert not any("status='finalizing'" in str(call.args[0]) for call in db.status.await_args_list)


@pytest.mark.asyncio
async def test_ordinary_import_keeps_existing_path(monkeypatch):
    monkeypatch.setattr(label_stage, "require_label_ordinary_publication", AsyncMock())
    db = database()
    assert await label_stage.protected_label_import({}, {}, db, None, None) is False
    db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_protected_worker_shutdown_never_publishes(monkeypatch):
    publish = AsyncMock()
    monkeypatch.setattr(label, "publish_label_table", publish)
    await label._label_shutdown_impl({"context": {"label_protected_mode": True}})
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_protected_startup_creates_no_ordinary_stage(monkeypatch):
    db = SimpleNamespace(status=AsyncMock(), create_table=AsyncMock())
    monkeypatch.setattr(label, "db", db)
    monkeypatch.setattr(label, "init_db", AsyncMock())
    monkeypatch.setattr(label, "is_label_publication_protected", AsyncMock(return_value=True))
    worker_context_dict = {}
    await label.label_startup(worker_context_dict)
    assert worker_context_dict["context"]["label_protected_mode"] is True
    db.create_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_protected_startup_requires_restart_before_ordinary_import(monkeypatch):
    """Ownership changes cannot enqueue batches into an uncreated ordinary heap."""
    db = SimpleNamespace(status=AsyncMock(), create_table=AsyncMock())
    monkeypatch.setattr(label, "db", db)
    monkeypatch.setattr(label, "init_db", AsyncMock())
    monkeypatch.setattr(label, "is_label_publication_protected", AsyncMock(return_value=True))
    monkeypatch.setattr(label_stage, "require_label_ordinary_publication", AsyncMock())
    pool = AsyncMock()
    download = AsyncMock()
    monkeypatch.setattr(label, "create_pool", pool)
    monkeypatch.setattr(label, "download_it", download)
    worker_context_dict = {}
    await label.label_startup(worker_context_dict)
    with pytest.raises(RuntimeError, match="restart the worker"):
        await label.init_label_file(worker_context_dict, {"run_id": "synthetic-run"})
    db.create_table.assert_not_awaited()
    pool.assert_not_awaited()
    download.assert_not_awaited()
    assert worker_context_dict["context"]["label_protected_mode"] is True


@pytest.mark.asyncio
async def test_publication_check_does_not_mask_unexpected_failures(monkeypatch):
    monkeypatch.setattr(
        label_stage, "require_label_ordinary_publication", AsyncMock(side_effect=RuntimeError("unavailable"))
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        await label_stage.is_label_publication_protected(database(), "synthetic")


@pytest.mark.asyncio
@pytest.mark.parametrize("task,batch_size", [({"test": True}, "100"), ({}, "0"), ({}, "1001")])
async def test_protected_source_limits_reject_before_claim(monkeypatch, task, batch_size):
    protect_label_source(monkeypatch, 1)
    monkeypatch.setenv("SAVE_PER_PACK", batch_size)
    db = database()
    with pytest.raises(ValueError, match="complete source|batch size"):
        await label_stage.protected_label_import({}, {"run_id": "synthetic-run", **task}, db, None, None)
    db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "total,partitions", [(0, []), (True, [{"records": 1}]), (1, []), (1, [{"records": 0}]), (2, [{"records": 1}])]
)
async def test_invalid_source_census_is_refused(monkeypatch, total, partitions):
    manifest_dict = {"results": {"drug": {"label": {"total_records": total, "partitions": partitions}}}}
    monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", "https://example.test/manifest")
    monkeypatch.setattr(
        label_stage,
        "download_it",
        AsyncMock(return_value=SimpleNamespace(content=json.dumps(manifest_dict), raise_for_status=lambda: None)),
    )
    with pytest.raises(ValueError, match="census"):
        await label_stage._read_label_manifest()


@pytest.mark.asyncio
async def test_failure_notification_errors_preserve_owned_failed_transition(monkeypatch):
    db = SimpleNamespace(status=AsyncMock(return_value=1))
    attempt = SimpleNamespace(schema="synthetic", run_id="synthetic-run", suffix="l" + "a" * 32, oid=None)
    cleanup = AsyncMock()
    monkeypatch.setattr(label_stage, "has_discarded_failed_label_stage", cleanup)
    monkeypatch.setattr(label_stage, "enqueue_status_event", Mock(side_effect=RuntimeError("unavailable")))
    monkeypatch.setattr(label_stage, "enqueue_live_progress", Mock(side_effect=RuntimeError("unavailable")))
    await label_stage._reconcile_failed_label_stage(db, attempt)
    assert "status='failed'" in str(db.status.await_args.args[0])
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity_dict",
    [
        None,
        {"oid": 72, "comment": "owned", "owned": True},
        {"oid": 71, "comment": "foreign", "owned": True},
        {"oid": 71, "comment": "owned", "owned": False},
    ],
)
async def test_failed_cleanup_refuses_replaced_or_foreign_heap(identity_dict):
    db = database()
    db.first = AsyncMock(return_value=identity_dict)
    with pytest.raises(RuntimeError, match="identity or owner changed"):
        await label_stage.has_discarded_failed_label_stage(
            db, "synthetic", "synthetic-run", "l" + "a" * 32, 71, "owned"
        )
    assert not any("DROP TABLE" in str(call.args[0]) for call in db.status.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("owned_run", [None, "synthetic-run"])
async def test_failed_cleanup_rechecks_owned_terminal_run(owned_run):
    db = database()
    db.scalar = AsyncMock(side_effect=[71, owned_run])
    db.first = AsyncMock(return_value={"oid": 71, "comment": "owned", "owned": True})
    assert await label_stage.has_discarded_failed_label_stage(
        db, "synthetic", "synthetic-run", "l" + "a" * 32, 71, "owned"
    ) is bool(owned_run)
    assert db.scalar.await_args.kwargs == {"run_id": "synthetic-run", "attempt_id": "l" + "a" * 32, "stage_oid": "71"}
    assert "label_publication_admission" in str(db.scalar.await_args.args[0])
    assert any("DROP TABLE" in str(call.args[0]) for call in db.status.await_args_list) is bool(owned_run)


@pytest.mark.asyncio
async def test_failed_cleanup_is_idempotent_for_absent_heap():
    db = database()
    db.scalar = AsyncMock(return_value=None)
    assert (
        await label_stage.has_discarded_failed_label_stage(
            db, "synthetic", "synthetic-run", "l" + "a" * 32, 71, "owned"
        )
        is False
    )
    with pytest.raises(ValueError, match="attempt identifier"):
        await label_stage.has_discarded_failed_label_stage(db, "synthetic", "synthetic-run", "foreign", 71, "owned")
    assert not any("DROP TABLE" in str(call.args[0]) for call in db.status.await_args_list)


@pytest.mark.asyncio
async def test_completed_handoff_survives_unavailable_status_event(monkeypatch):
    protect_label_source(monkeypatch, 1)
    monkeypatch.setattr(label_stage, "_save_label_partitions", AsyncMock())
    receipt_dict = {"format": "label-completed-stage-v1", "run_id": "synthetic-run"}
    monkeypatch.setattr(label_stage, "finish_label_stage", AsyncMock(return_value=receipt_dict))
    monkeypatch.setattr(label_stage, "enqueue_status_event", Mock(side_effect=RuntimeError("unavailable")))
    cleanup = AsyncMock()
    monkeypatch.setattr(label_stage, "_reconcile_failed_label_stage", cleanup)
    assert (
        await label_stage.protected_label_import({}, {"run_id": "synthetic-run"}, database(), None, None)
        == receipt_dict
    )
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [None, {"run_id": "existing", "status": "finalizing"}])
async def test_protected_admission_serializes_and_reuses_active_run(monkeypatch, active):
    """Reuse protected active work before admitting and committing a new run."""
    session = SimpleNamespace(commit=AsyncMock())

    @asynccontextmanager
    async def admission_session():
        """Expose the commit fence without opening a database connection."""
        yield session

    db = SimpleNamespace(
        session=admission_session, status=AsyncMock(), scalar=AsyncMock(), first=AsyncMock(return_value=active)
    )
    insert = AsyncMock()
    monkeypatch.setattr(control_imports, "db", db)
    monkeypatch.setattr(control_imports, "is_label_publication_protected", AsyncMock(return_value=True))
    monkeypatch.setattr(control_imports, "insert_import_run", insert)
    result = await control_imports._admit_label_run("synthetic", {"run_id": "new"})
    assert "ROW EXCLUSIVE MODE" in str(db.status.await_args.args[0])
    db.scalar.assert_awaited_once()
    if active:
        assert result["run_id"] == "existing"
        insert.assert_not_awaited()
        session.commit.assert_not_awaited()
    else:
        assert result is None
        insert.assert_awaited_once_with("synthetic", {"run_id": "new"})
        session.commit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("callback", [label.download_label_content, label.process_label_results])
async def test_old_queue_batches_cannot_mutate_a_protected_attempt(callback):
    with pytest.raises(RuntimeError, match="coordinator-owned"):
        await callback({"context": {"label_protected_mode": True}}, {"results": []})


@pytest.mark.asyncio
async def test_protected_standalone_attempt_fails_closed(monkeypatch):
    monkeypatch.setattr(
        label_stage,
        "require_label_ordinary_publication",
        AsyncMock(side_effect=RuntimeError("ordinary Label publication is disabled for the protected live table")),
    )
    db = database()
    with pytest.raises(ValueError, match="machine identifier"):
        await label_stage.protected_label_import({"import_date": "l" + "a" * 32}, {}, db, None, None)
    db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("actual_rows", [1, 2])
async def test_coordinator_awaits_every_partition_and_rejects_incomplete_census(monkeypatch, actual_rows):
    protect_label_source(monkeypatch, 2)
    monkeypatch.setattr(label_stage, "lock_label_stage", AsyncMock())
    cleanup = AsyncMock()
    monkeypatch.setattr(label_stage, "has_discarded_failed_label_stage", cleanup)

    async def download(ctx, _task, _spec):
        await ctx["redis"].enqueue_job(
            "process_label_results",
            {
                "results": [{"id": str(n)} for n in range(actual_rows)],
            },
        )

    monkeypatch.setattr(label_stage, "download_partition_content", download)
    sealed_receipt_dict = {"format": "label-completed-stage-v1", "run_id": "synthetic-run"}
    finalize = AsyncMock(return_value=sealed_receipt_dict)
    monkeypatch.setattr(label_stage, "finish_label_stage", finalize)
    db = database()
    db.scalar = AsyncMock(return_value=71)
    context_dict = {"import_date": "l" + "b" * 32}
    if actual_rows == 1:
        with pytest.raises(RuntimeError, match="partition census"):
            await label_stage.protected_label_import(
                context_dict, {"run_id": "synthetic-run"}, db, label._label_row_dict_from_record, None
            )
        finalize.assert_not_awaited()
        assert "status='failed'" in str(db.status.await_args_list[-1].args[0])
        cleanup.assert_awaited_once()
    else:
        assert (
            await label_stage.protected_label_import(
                context_dict, {"run_id": "synthetic-run"}, db, label._label_row_dict_from_record, None
            )
            == sealed_receipt_dict
        )
        assert finalize.await_args.args[-1] == 2
        assert context_dict["context"]["label_protected_mode"] is True


def recording_stage_database(monkeypatch):
    attempt_by_run = {}
    stage_by_name = {}

    async def status(statement, **params):
        if "jsonb_build_object('label_attempt_id'" in str(statement):
            if params["run_id"] in attempt_by_run:
                return 0
            attempt_by_run[params["run_id"]] = params["attempt_id"]
        return 1

    async def create(callback):
        table = callback.__self__
        assert table.name not in stage_by_name
        stage_by_name[table.name] = {"oid": len(stage_by_name) + 71, "rows": []}

    async def execute(statement, rows):
        stage_by_name[statement.table.name]["rows"].extend(rows)

    @asynccontextmanager
    async def stage_transaction():
        yield SimpleNamespace(execute=execute, connection=AsyncMock(return_value=SimpleNamespace(run_sync=create)))

    async def scalar(_statement, **params):
        return stage_by_name[params["name"].split(".")[1]]["oid"]

    async def check_owner(_db, _schema, run_id, suffix, oid, _comment):
        assert attempt_by_run[run_id] == suffix
        assert stage_by_name["label_" + suffix]["oid"] == oid

    async def finalize(db, schema, run_id, suffix, oid, comment, expected):
        await check_owner(db, schema, run_id, suffix, oid, comment)
        rows = stage_by_name["label_" + suffix]["rows"]
        assert [row["id"] for row in rows] == [run_id]
        assert len(rows) == expected
        return {"run_id": run_id, "attempt_id": suffix, "relation_oid": oid}

    monkeypatch.setattr(label_stage, "lock_label_stage", check_owner)
    monkeypatch.setattr(label_stage, "finish_label_stage", finalize)
    return SimpleNamespace(transaction=stage_transaction, status=status, scalar=scalar), stage_by_name


@pytest.mark.asyncio
async def test_two_runs_on_one_worker_create_distinct_owned_stages(monkeypatch):
    protect_label_source(monkeypatch, 1)
    db, stage_by_name = recording_stage_database(monkeypatch)
    download_barrier = asyncio.Barrier(2)

    async def download(ctx, task, _spec):
        await asyncio.wait_for(download_barrier.wait(), timeout=2)
        await ctx["redis"].enqueue_job("process_label_results", {"results": [{"id": task["run_id"]}]})

    monkeypatch.setattr(label_stage, "download_partition_content", download)
    worker_context_dict = {"import_date": "ordinary-worker-stage", "context": {}}
    receipts = await asyncio.gather(
        *(
            label_stage.protected_label_import(
                dict(worker_context_dict), {"run_id": run_id}, db, label._label_row_dict_from_record, None
            )
            for run_id in ("synthetic-one", "synthetic-two")
        )
    )
    assert len({receipt["attempt_id"] for receipt in receipts}) == 2
    assert len({receipt["relation_oid"] for receipt in receipts}) == 2
    assert worker_context_dict["import_date"] == "ordinary-worker-stage"
    assert len(stage_by_name) == 2
    with pytest.raises(RuntimeError, match="no longer claimable"):
        await label_stage.protected_label_import(
            dict(worker_context_dict), {"run_id": "synthetic-one"}, db, label._label_row_dict_from_record, None
        )
    assert len(stage_by_name) == 2


def test_long_timeout_is_limited_to_label_coordinators():
    from process import Labeling

    assert not hasattr(Labeling, "job_timeout")
    assert Labeling.functions[:2] == [label.download_label_content, label.process_label_results]
    assert [function.timeout_s for function in Labeling.functions[2:]] == [86400, 86400]


def test_start_label_click_dispatch(monkeypatch):
    import process

    start = AsyncMock()
    monkeypatch.setattr(process, "initiate_label_import", start)
    assert CliRunner().invoke(process.process_group, ["label"]).exit_code == 0
    start.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("protected,created", [(False, True), (True, True), (True, False)])
async def test_standalone_label_command_uses_managed_run_only_when_protected(monkeypatch, protected, created):
    worker_db = SimpleNamespace(disconnect=AsyncMock())
    monkeypatch.setattr(label, "db", worker_db)
    monkeypatch.setattr(label, "init_db", AsyncMock())
    monkeypatch.setattr(label, "is_label_publication_protected", AsyncMock(return_value=protected))
    redis = SimpleNamespace(enqueue_job=AsyncMock())
    pool = AsyncMock(return_value=redis)
    monkeypatch.setattr(label, "create_pool", pool)
    create = AsyncMock(return_value=({"status": "queued"}, created))
    monkeypatch.setattr(control_imports, "create_import_run", create)
    if protected and not created:
        with pytest.raises(RuntimeError, match="could not be queued"):
            await label.main()
    else:
        await label.main()
    worker_db.disconnect.assert_awaited_once()
    if protected:
        create.assert_awaited_once_with({"importer": "label", "triggered_by": "cli"})
        pool.assert_not_awaited()
    else:
        create.assert_not_awaited()
        redis.enqueue_job.assert_awaited_once_with("init_label_file")
