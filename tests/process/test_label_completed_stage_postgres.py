"""Actual Label stage transactions, OIDs and index receipts on disposable PostgreSQL."""

import asyncio
import importlib
import json
import os
from contextlib import AsyncExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text

from api import control_imports, control_run_store
from db.connection import Database
from process import label_stage

label = importlib.import_module("process.label")


@pytest.fixture
async def stage_database(monkeypatch):
    if os.getenv("HLTHPRT_ENVIRONMENT") != "test" or "test" not in os.getenv("HLTHPRT_DB_DATABASE", ""):
        pytest.skip("requires a disposable PostgreSQL test database")
    database = Database()
    schema = "label_stage_test_" + uuid4().hex
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(database.disconnect)
        await database.connect()

        async def drop_schema():
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            assert await database.scalar("SELECT to_regnamespace(:schema)", schema=schema) is None

        cleanup.push_async_callback(drop_schema)
        await database.status(f"CREATE SCHEMA {schema}")
        await database.status(f"""
            CREATE TABLE {schema}.import_run (
                run_id text PRIMARY KEY, engine text, node_id text, importer text, family text,
                status text, phase_detail text, params jsonb, idempotency_key text,
                triggered_by text, schedule_id text, created_at timestamp, started_at timestamp,
                heartbeat_at timestamp, finished_at timestamp, progress jsonb,
                metrics jsonb DEFAULT '{{}}', error jsonb, import_id text, retry_of_run_id text
            )
        """)
        monkeypatch.setenv("DB_SCHEMA", schema)
        monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", "https://example.test/manifest")
        monkeypatch.setattr(label_stage, "ensure_import_run_table", AsyncMock())
        # Ownership detection is covered separately; all staging SQL below is real.
        monkeypatch.setattr(
            label_stage,
            "require_label_ordinary_publication",
            AsyncMock(side_effect=RuntimeError("ordinary Label publication is disabled for the protected live table")),
        )
        monkeypatch.setattr(label_stage, "enqueue_status_event", lambda _event: None)
        manifest_dict = {
            "results": {
                "drug": {
                    "label": {
                        "total_records": 1,
                        "partitions": [{"file": "https://example.test/part.zip", "records": 1}],
                    }
                }
            }
        }
        monkeypatch.setattr(
            label_stage,
            "download_it",
            AsyncMock(return_value=SimpleNamespace(content=json.dumps(manifest_dict), raise_for_status=lambda: None)),
        )
        yield database, schema


async def configure_control_database(database, monkeypatch):
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_run_store, "db", database)
    await control_imports.ensure_import_run_table()
    monkeypatch.setattr(control_imports, "ensure_import_run_table", AsyncMock())


def queued_update():
    return {
        "status": "queued",
        "phase_detail": "enqueued",
        "heartbeat_at": None,
        "progress": {},
        "metrics": {},
        "error": None,
    }


@pytest.mark.asyncio
async def test_protected_admission_allows_one_active_label_run(stage_database, monkeypatch):
    database, schema = stage_database
    await configure_control_database(database, monkeypatch)
    protected = AsyncMock(return_value=False)
    monkeypatch.setattr(control_imports, "is_label_publication_protected", protected)
    monkeypatch.setattr(control_imports, "_enqueue", AsyncMock(return_value=queued_update()))
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda _event: None)
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: None)
    lock_attempts = []
    at_lock = asyncio.Event()
    status = database.status

    async def observe_lock(statement, **params):
        if "LOCK TABLE ONLY" in str(statement):
            lock_attempts.append(statement)
            if len(lock_attempts) == 2:
                at_lock.set()
        return await status(statement, **params)

    monkeypatch.setattr(database, "status", observe_lock)
    requests = ({"importer": "label", "run_id": f"synthetic-{n}", "idempotency_key": f"key-{n}"} for n in range(2))
    pending_tasks = []
    try:
        async with database.engine.connect() as connection:
            async with connection.begin():
                await connection.execute(text(f"LOCK TABLE ONLY {schema}.import_run IN SHARE MODE"))
                pending_tasks = [
                    asyncio.create_task(control_imports.create_import_run(request)) for request in requests
                ]
                await asyncio.wait_for(at_lock.wait(), timeout=5)
                protected.assert_not_awaited()
                protected.return_value = True
        admission_results = await asyncio.gather(*pending_tasks)
    finally:
        for task in pending_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pending_tasks, return_exceptions=True)
    assert sorted(created for _run, created in admission_results) == [False, True]
    assert len({run["run_id"] for run, _created in admission_results}) == 1
    assert await database.scalar(f"SELECT count(*) FROM {schema}.import_run WHERE importer='label'") == 1
    active_run = admission_results[0][0]["run_id"]
    await database.status(f"UPDATE {schema}.import_run SET status='succeeded' WHERE run_id=:run_id", run_id=active_run)
    replacement, created = await control_imports.create_import_run({"importer": "label", "run_id": "synthetic-next"})
    assert created and replacement["run_id"] == "synthetic-next"
    control_imports.is_label_publication_protected.return_value = False
    ordinary, created = await control_imports.create_import_run({"importer": "label", "run_id": "synthetic-ordinary"})
    assert created and ordinary["run_id"] == "synthetic-ordinary"


@pytest.mark.asyncio
async def test_cancel_while_label_enqueue_is_pending_stays_canceled(stage_database, monkeypatch):
    database, schema = stage_database
    await configure_control_database(database, monkeypatch)
    monkeypatch.setattr(control_imports, "is_label_publication_protected", AsyncMock(return_value=True))
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run_id: None)
    monkeypatch.setattr(control_imports, "_remove_queued_job", AsyncMock(return_value={"removed": False}))
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: None)
    events = []
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda event: events.append(event["status"]))
    enqueue_started = asyncio.Event()
    finish_enqueue = asyncio.Event()

    async def delayed_enqueue(_spec, _run):
        enqueue_started.set()
        await finish_enqueue.wait()
        return queued_update()

    monkeypatch.setattr(control_imports, "_enqueue", delayed_enqueue)
    creation = asyncio.create_task(
        control_imports.create_import_run(
            {
                "importer": "label",
                "run_id": "synthetic-cancel-during-enqueue",
            }
        )
    )
    try:
        await asyncio.wait_for(enqueue_started.wait(), timeout=5)
        canceled = await control_imports.request_cancel("synthetic-cancel-during-enqueue")
        assert canceled["status"] == "canceled"
    finally:
        finish_enqueue.set()
        run, created = await creation
    assert created and run["status"] == "canceled"
    stored = await database.first(
        f"SELECT status,finished_at FROM {schema}.import_run WHERE run_id=:run_id",
        run_id="synthetic-cancel-during-enqueue",
    )
    assert stored["status"] == "canceled" and stored["finished_at"] is not None
    assert events == ["canceled"]
    monkeypatch.setattr(label_stage, "is_label_publication_protected", AsyncMock(return_value=True))
    with pytest.raises(RuntimeError, match="no longer claimable"):
        await label_stage.protected_label_import(
            {"context": {}},
            {"run_id": "synthetic-cancel-during-enqueue"},
            database,
            label._label_row_dict_from_record,
            None,
        )
    assert (
        await database.scalar(
            "SELECT count(*) FROM pg_class WHERE relnamespace=to_regnamespace(:schema) AND relname LIKE 'label_%'",
            schema=schema,
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("advanced_status", ["running", "succeeded", "canceled"])
async def test_stale_label_queued_cancel_does_not_overwrite_advanced_state(
    stage_database,
    monkeypatch,
    advanced_status,
):
    database, schema = stage_database
    monkeypatch.setattr(control_imports, "db", database)
    await control_imports.ensure_import_run_table()
    monkeypatch.setattr(control_imports, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run_id: None)
    await database.status(
        f"INSERT INTO {schema}.import_run(run_id,importer,status) VALUES ('synthetic-stale','label','queued')",
    )
    stale = await control_imports.get_import_run("synthetic-stale")
    await database.status(
        f"UPDATE {schema}.import_run SET status=:status WHERE run_id='synthetic-stale'",
        status=advanced_status,
    )
    current = await control_imports.get_import_run("synthetic-stale")
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(side_effect=[stale, current]))
    monkeypatch.setattr(control_imports, "_remove_queued_job", AsyncMock(return_value={"removed": False}))
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda _event: pytest.fail("stale event"))
    monkeypatch.setattr(
        control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: pytest.fail("stale progress")
    )
    assert await control_imports.request_cancel("synthetic-stale") == current
    assert (
        await database.scalar(f"SELECT status FROM {schema}.import_run WHERE run_id='synthetic-stale'")
        == advanced_status
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("advanced_status", ["running", "succeeded", "canceled"])
async def test_late_label_enqueue_ack_does_not_overwrite_advanced_state(stage_database, monkeypatch, advanced_status):
    database, schema = stage_database
    monkeypatch.setattr(control_run_store, "db", database)
    await database.status(
        f"INSERT INTO {schema}.import_run(run_id,importer,status) VALUES ('synthetic-ack','label',:status)",
        status=advanced_status,
    )
    changed = await control_run_store.update_import_run_after_enqueue(
        schema,
        "synthetic-ack",
        {
            "status": "queued",
            "phase_detail": "enqueued",
            "heartbeat_at": None,
            "progress": {},
            "metrics": {},
            "error": None,
        },
    )
    assert changed == 0
    assert (
        await database.scalar(f"SELECT status FROM {schema}.import_run WHERE run_id='synthetic-ack'") == advanced_status
    )


async def assert_completed_stage(database, schema, receipt):
    table = f"{schema}.{receipt['table']}"
    assert await database.scalar("SELECT to_regclass(:table)::oid", table=table) == receipt["relation_oid"]
    assert await database.scalar(f"SELECT id FROM {table}") == receipt["run_id"]
    run = await database.first(
        f"SELECT status,metrics,finished_at FROM {schema}.import_run WHERE run_id=:run", run=receipt["run_id"]
    )
    assert run["status"] == "finalizing" and run["finished_at"] is None
    assert run["metrics"]["label_completed_stage"] == receipt
    assert len(receipt["indexes"]) == 5 and receipt["indexes"][0]["primary"] is True
    assert all(index["valid"] and index["ready"] for index in receipt["indexes"])
    assert sum(index["primary"] for index in receipt["indexes"]) == 1
    assert receipt["row_count"] == receipt["source_row_count"] == 1
    expected_comment_dict = {
        "format": "label-stage-v1",
        "run_id": receipt["run_id"],
        "attempt_id": receipt["attempt_id"],
    }
    assert (
        json.loads(await database.scalar("SELECT obj_description(:oid,'pg_class')", oid=receipt["relation_oid"]))
        == expected_comment_dict
    )


@pytest.mark.asyncio
async def test_completed_handoff_retains_progress_in_get_and_list(stage_database, monkeypatch):
    database, schema = stage_database
    await configure_control_database(database, monkeypatch)
    await database.status(
        f"INSERT INTO {schema}.import_run(run_id,importer,status) VALUES ('synthetic-handoff','label','running')"
    )

    async def download(ctx, _task, _spec):
        await ctx["redis"].enqueue_job("process_label_results", {"results": [{"id": "synthetic-label"}]})

    monkeypatch.setattr(label_stage, "download_partition_content", download)
    receipt = await label_stage.protected_label_import(
        {"context": {}}, {"run_id": "synthetic-handoff"}, database, label._label_row_dict_from_record, None
    )
    persisted_run = await database.first(
        f"SELECT status,progress,phase_detail FROM {schema}.import_run WHERE run_id='synthetic-handoff'"
    )
    assert persisted_run["status"] == "finalizing"
    assert persisted_run["progress"] == {
        "unit": "stage",
        "total": 1,
        "done": 1,
        "pct": 100,
        "message": "stage complete; awaiting protected publication",
    }
    assert persisted_run["phase_detail"] == "label stage ready for protected publication"
    stale_progress_dict = {
        "run_id": "synthetic-handoff",
        "status": "running",
        "phase": "downloading",
        "unit": "records",
        "total": 1,
        "done": 0,
        "pct": 0,
        "message": "still downloading",
        "updated_at": control_imports.utc_now().isoformat(),
    }
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run_id: stale_progress_dict)
    fetched_run = await control_imports.get_import_run("synthetic-handoff")
    listed_runs = await control_imports.list_import_runs(importer="label")
    assert len(listed_runs) == 1
    for visible_run in (fetched_run, listed_runs[0]):
        assert visible_run["status"] == "finalizing"
        assert visible_run["progress"] == persisted_run["progress"]
        assert visible_run["phase_detail"] == persisted_run["phase_detail"]
        assert visible_run["metrics"]["label_completed_stage"] == receipt


@pytest.mark.asyncio
async def test_concurrent_completed_stages_and_rejected_claim_leave_exact_rows(stage_database, monkeypatch):
    database, schema = stage_database
    runs = ("synthetic-one", "synthetic-two")
    for run_id in runs:
        await database.status(
            f"INSERT INTO {schema}.import_run(run_id,importer,status) VALUES (:run,'label','running')", run=run_id
        )
    download_barrier = asyncio.Barrier(2)

    async def download(ctx, task, _spec):
        await asyncio.wait_for(download_barrier.wait(), timeout=5)
        await ctx["redis"].enqueue_job("process_label_results", {"results": [{"id": task["run_id"]}]})

    monkeypatch.setattr(label_stage, "download_partition_content", download)
    finalize = label_stage.finish_label_stage

    async def finish_with_primary_only(db, stage_schema, run_id, suffix, oid, comment, expected):
        indexes = await db.all("SELECT indisprimary FROM pg_index WHERE indrelid=:oid", oid=oid)
        assert len(indexes) == 1 and indexes[0]["indisprimary"] is True
        return await finalize(db, stage_schema, run_id, suffix, oid, comment, expected)

    monkeypatch.setattr(label_stage, "finish_label_stage", finish_with_primary_only)
    worker_context_dict = {"import_date": "ordinary-stage", "context": {}}
    receipts = await asyncio.gather(
        *(
            label_stage.protected_label_import(
                dict(worker_context_dict), {"run_id": run_id}, database, label._label_row_dict_from_record, None
            )
            for run_id in runs
        )
    )
    assert len({receipt["attempt_id"] for receipt in receipts}) == 2
    assert len({receipt["relation_oid"] for receipt in receipts}) == 2
    for receipt in receipts:
        await assert_completed_stage(database, schema, receipt)
    before = await database.all(
        "SELECT oid,relname FROM pg_class WHERE relnamespace=to_regnamespace(:schema) ORDER BY oid", schema=schema
    )
    with pytest.raises(RuntimeError, match="no longer claimable"):
        await label_stage.protected_label_import(
            dict(worker_context_dict), {"run_id": runs[0]}, database, label._label_row_dict_from_record, None
        )
    after = await database.all(
        "SELECT oid,relname FROM pg_class WHERE relnamespace=to_regnamespace(:schema) ORDER BY oid", schema=schema
    )
    assert before == after
    assert await database.scalar(f"SELECT count(*) FROM {schema}.import_run WHERE status='finalizing'") == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["oid_read", "oid_record"])
async def test_creation_failure_rolls_back_claim_and_owned_ddl(stage_database, monkeypatch, failure):
    database, schema = stage_database
    await database.status(
        f"INSERT INTO {schema}.import_run(run_id,importer,status) VALUES ('synthetic-failed','label','running')"
    )
    scalar = database.scalar

    async def fail_after_create(statement, **params):
        if failure == "oid_read" and "to_regclass" in str(statement):
            raise RuntimeError("synthetic failure after table creation")
        return await scalar(statement, **params)

    monkeypatch.setattr(database, "scalar", fail_after_create)
    status = database.status

    async def lose_oid_record(statement, **params):
        if failure == "oid_record" and "jsonb_build_object('label_stage_oid'" in str(statement):
            return 0
        return await status(statement, **params)

    monkeypatch.setattr(database, "status", lose_oid_record)
    with pytest.raises(RuntimeError, match="after table creation|identity could not be recorded"):
        await label_stage.protected_label_import(
            {"context": {}}, {"run_id": "synthetic-failed"}, database, label._label_row_dict_from_record, None
        )
    run = await database.first(f"SELECT status,metrics FROM {schema}.import_run WHERE run_id='synthetic-failed'")
    assert run["status"] == "running" and run["metrics"] == {}
    assert (
        await scalar(
            "SELECT count(*) FROM pg_class WHERE relnamespace=to_regnamespace(:schema) AND relname LIKE 'label_%'",
            schema=schema,
        )
        == 0
    )


async def apply_cleanup_fence(database, schema, fence, stage_dict):
    if fence in {"admitted", "handoff"}:
        key = "label_publication_admission" if fence == "admitted" else "label_completed_stage"
        await database.status(
            f"UPDATE {schema}.import_run SET metrics=metrics || jsonb_build_object(CAST(:key AS text),'{{}}'::jsonb)",
            key=key,
        )
        return
    if fence == "foreign_comment":
        await database.status(f"COMMENT ON TABLE {stage_dict['name']} IS 'synthetic-foreign-owner'")
        return
    if fence == "protected_owner":
        await database.status(f"ALTER TABLE {stage_dict['name']} OWNER TO pg_database_owner")
        return
    if fence == "finalizing":
        await database.status(f"UPDATE {schema}.import_run SET status='finalizing'")
        return
    if fence == "oid_record":
        await database.status(
            f"UPDATE {schema}.import_run SET metrics=metrics || jsonb_build_object('label_stage_oid', 0)"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fence",
    [
        None,
        "admitted",
        "handoff",
        "foreign_comment",
        "protected_owner",
        "finalizing",
        "oid_record",
        "cleanup_unavailable",
    ],
)
async def test_failed_owned_stage_cleanup_preserves_admitted_or_changed_heaps(stage_database, monkeypatch, fence):
    database, schema = stage_database
    await database.status(f"CREATE TABLE {schema}.label (id text PRIMARY KEY)")
    await database.status(f"INSERT INTO {schema}.label VALUES ('synthetic-incumbent')")
    incumbent_oid = await database.scalar("SELECT to_regclass(:name)::oid", name=f"{schema}.label")
    await database.status(
        f"INSERT INTO {schema}.import_run(run_id,importer,status) VALUES ('synthetic-failure','label','running')"
    )
    stage_dict = {}
    cleanup = label_stage.has_discarded_failed_label_stage
    if fence == "cleanup_unavailable":
        monkeypatch.setattr(
            label_stage, "has_discarded_failed_label_stage", AsyncMock(side_effect=TimeoutError("synthetic timeout"))
        )

    async def fail_after_save(ctx, _task, _spec):
        await ctx["redis"].enqueue_job("process_label_results", {"results": [{"id": "synthetic-partial"}]})
        suffix = await database.scalar(
            f"SELECT metrics->>'label_attempt_id' FROM {schema}.import_run WHERE run_id='synthetic-failure'"
        )
        stage_dict["name"] = f"{schema}.label_{suffix}"
        stage_dict["suffix"] = suffix
        stage_dict["oid"] = await database.scalar("SELECT to_regclass(:name)::oid", name=stage_dict["name"])
        await apply_cleanup_fence(database, schema, fence, stage_dict)
        raise RuntimeError("synthetic source failure after committed save")

    monkeypatch.setattr(label_stage, "download_partition_content", fail_after_save)
    with pytest.raises(RuntimeError, match="source failure"):
        await label_stage.protected_label_import(
            {"context": {}}, {"run_id": "synthetic-failure"}, database, label._label_row_dict_from_record, None
        )
    current_oid = await database.scalar("SELECT to_regclass(:name)::oid", name=stage_dict["name"])
    assert current_oid == (stage_dict["oid"] if fence else None)
    if fence:
        assert await database.scalar(f"SELECT id FROM {stage_dict['name']}") == "synthetic-partial"
    assert await database.scalar("SELECT to_regclass(:name)::oid", name=f"{schema}.label") == incumbent_oid
    assert await database.scalar(f"SELECT id FROM {schema}.label") == "synthetic-incumbent"
    status = await database.scalar(f"SELECT status FROM {schema}.import_run WHERE run_id='synthetic-failure'")
    assert status == ("finalizing" if fence == "finalizing" else "failed")
    if fence == "cleanup_unavailable":
        metrics = await database.scalar(f"SELECT metrics FROM {schema}.import_run WHERE run_id='synthetic-failure'")
        assert metrics["label_stage_oid"] == stage_dict["oid"]
        comment = json.dumps(
            {"format": "label-stage-v1", "run_id": "synthetic-failure", "attempt_id": stage_dict["suffix"]},
            sort_keys=True,
        )
        async with asyncio.timeout(10):
            assert await cleanup(
                database, schema, "synthetic-failure", stage_dict["suffix"], metrics["label_stage_oid"], comment
            )
        assert await database.scalar("SELECT to_regclass(:name)::oid", name=stage_dict["name"]) is None
