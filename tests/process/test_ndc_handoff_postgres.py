"""Synthetic native handoff transactions through the actual managed worker."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import uuid4

import pytest
from arq.jobs import deserialize_result, serialize_job
from arq.worker import create_worker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker

from api import control_imports
from db.connection import current_session
from process import NDC, control_lifecycle, ndc_handoff, ndc_product, ndc_publish, ndc_stage
from tests.process.ndc_handoff_role_fixtures import handoff_reader_case
from tests.process.ndc_publication_fixtures import install_coordinator_sources
from tests.process.test_ndc_publication_proof_postgres import (
    _acquisition,
    _pair_state,
    _run_state,
    _save_one,
)
from tests.process.test_ndc_publication_proof_postgres import (
    publication_case as publication_case,
)


async def _handoff(case):
    return await ndc_publish.publish_ndc_tables(
        case.database,
        case.schema,
        case.attempt.suffix,
        attempt=case.attempt,
        acquisition=_acquisition(case),
        publication_mode="handoff",
    )


async def _coordinator_case(case, monkeypatch):
    run_id = "synthetic-worker-" + uuid4().hex
    await case.database.status(
        f"""
        INSERT INTO {case.schema}.import_run (run_id, engine, importer, status, import_id)
        VALUES (:run_id, 'drug-api', 'ndc', 'queued', 'synthetic-source')
    """,
        run_id=run_id,
    )
    for module in (ndc_product, control_lifecycle, control_imports):
        monkeypatch.setattr(module, "db", case.database)
    events = []
    for module in (ndc_product, control_lifecycle):
        monkeypatch.setattr(module, "enqueue_live_progress", Mock())
        monkeypatch.setattr(module, "enqueue_status_event", events.append)
    manifest_url = "https://example.test/ndc/manifest.json"
    _, _, temporary_paths = install_coordinator_sources(monkeypatch, manifest_url)
    monkeypatch.setenv("DB_SCHEMA", case.schema)
    monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", manifest_url)
    monkeypatch.setenv("HLTHPRT_NDC_PUBLICATION_MODE", "handoff")
    return SimpleNamespace(run_id=run_id, events=events, temporary_paths=temporary_paths)


async def _run_worker(run_id, monkeypatch):
    task_dict = {
        "run_id": run_id,
        "importer": "ndc",
        "target_module": "process.ndc_product",
        "target_function": "init_file",
        "call_style": "ctx_task",
        "task": {},
    }
    pipeline = MagicMock()
    pipeline.__aenter__.return_value = pipeline
    pipeline.execute = AsyncMock(
        return_value=[
            serialize_job("control_single_job_start", (task_dict,), {}, None, 0, serializer=NDC.job_serializer),
            1,
            True,
        ]
    )
    redis = MagicMock()
    redis.pipeline.return_value = pipeline
    worker = create_worker(NDC, redis_pool=redis, handle_signals=False, keep_result=60)
    monkeypatch.setattr(worker, "finish_job", AsyncMock())
    await worker.run_job("synthetic-job", 0)
    assert worker.jobs_complete == 1 and worker.jobs_failed == worker.jobs_retried == 0
    assert not worker.job_tasks
    result = deserialize_result(worker.finish_job.call_args.args[2], deserializer=NDC.job_deserializer)
    assert result.success is True
    return result.result


@pytest.mark.asyncio
async def test_worker_handoff_is_durable_and_nonterminal(publication_case, monkeypatch):
    case = publication_case
    coordinator = await _coordinator_case(case, monkeypatch)
    before = await _pair_state(case)
    previous = await _pair_state(case, "_old")
    worker_result = await _run_worker(coordinator.run_id, monkeypatch)
    assert worker_result["status"] == "finalizing"
    receipt = worker_result["result"]
    assert ndc_handoff.has_valid_ndc_handoff(receipt, coordinator.run_id)
    with pytest.raises(RuntimeError, match="No SQLAlchemy session"):
        current_session()
    async with case.database.engine.connect() as independent:
        run = (
            (
                await independent.exec_driver_sql(
                    f"SELECT * FROM {case.schema}.import_run WHERE run_id='{coordinator.run_id}'"
                )
            )
            .mappings()
            .one()
        )
    assert run["status"] == "finalizing" and run["finished_at"] is None and run["import_id"] == "synthetic-source"
    assert run["metrics"]["ndc_handoff"] == receipt and "ndc_publication" not in run["metrics"]
    assert await _pair_state(case) == before and await _pair_state(case, "_old") == previous
    stages = await _pair_state(case, "_" + receipt["attempt_id"])
    for name, state in stages.items():
        assert len(state["indexes"]) == (5 if name == "product" else 2)
        assert all(index[2] for index in state["indexes"])
        for key in ("oid", "row_count", "sha256", "size_bytes"):
            assert receipt["tables"][name][key] == state[key]
        assert json.loads(state["comment"])["handoff_sha256"] == receipt["handoff_sha256"]
    assert all(event["status"] != "succeeded" for event in coordinator.events)
    assert coordinator.events[-1]["status"] == "finalizing"
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run: pytest.fail("stale handoff overlay"))
    assert (await control_imports.get_import_run(coordinator.run_id))[
        "phase_detail"
    ] == "ndc stages awaiting publication"
    assert len(coordinator.temporary_paths) == 3 and all(not path.exists() for path in coordinator.temporary_paths)


@pytest.mark.asyncio
@pytest.mark.parametrize("later_state", ["finalizing", "failed", "canceled", "running", "missing", "comment"])
async def test_handoff_retains_stages_against_old_worker(publication_case, later_state):
    case = publication_case
    await _save_one(case)
    receipt = await _handoff(case)
    if later_state == "missing":
        await case.database.status(
            f"DELETE FROM {case.schema}.import_run WHERE run_id=:run_id", run_id=case.attempt.run_id
        )
    elif later_state == "comment":
        await case.database.status(
            f"COMMENT ON TABLE {case.schema}.product_{case.attempt.suffix} IS 'synthetic consumer'"
        )
    else:
        await case.database.status(
            f"UPDATE {case.schema}.import_run SET status=:status WHERE run_id=:run_id",
            status=later_state,
            run_id=case.attempt.run_id,
        )
        # The durable transfer still fences an old worker if comments are reset.
        for table in case.attempt.tables.values():
            await ndc_stage._comment_ndc_table(case.database, case.schema, table.name, case.attempt.owner_comment)
    before = await _pair_state(case, "_" + case.attempt.suffix)
    assert await ndc_stage.fail_ndc_attempt(case.database, case.attempt) == 0
    for action in (_save_one, _handoff):
        with pytest.raises(RuntimeError):
            await action(case)
    with pytest.raises(RuntimeError):
        await ndc_stage.discard_ndc_stages(case.database, case.attempt)
    assert await _pair_state(case, "_" + case.attempt.suffix) == before
    assert receipt["published"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["comment", "cancel", "run-cas"])
async def test_handoff_failure_rolls_back_pair_markers(publication_case, monkeypatch, failure):
    case = publication_case
    await _save_one(case)
    before = await _pair_state(case, "_" + case.attempt.suffix)
    native = await _pair_state(case)
    original_status = case.database.status

    async def fail_metadata(statement, **parameters):
        if str(statement).startswith(f"COMMENT ON TABLE {case.schema}.package_"):
            raise asyncio.CancelledError() if failure == "cancel" else RuntimeError("synthetic comment failure")
        if failure == "run-cas" and "SET status='finalizing'" in str(statement):
            return 0
        return await original_status(statement, **parameters)

    monkeypatch.setattr(case.database, "status", fail_metadata)
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
        await _handoff(case)
    assert await _pair_state(case, "_" + case.attempt.suffix) == before
    assert await _pair_state(case) == native
    assert (await _run_state(case))["status"] == "running"


@pytest.mark.asyncio
async def test_lost_commit_ack_preserves_handoff(publication_case, monkeypatch):
    case = publication_case
    coordinator = await _coordinator_case(case, monkeypatch)
    original_transaction = case.database.transaction
    original_handoff = ndc_publish.handoff_ndc_stages
    ack_state = SimpleNamespace(is_armed=False)

    async def arm_lost_ack(*args):
        receipt = await original_handoff(*args)
        ack_state.is_armed = True
        return receipt

    @asynccontextmanager
    async def lose_commit_ack():
        async with original_transaction() as session:
            yield session
        if ack_state.is_armed:
            ack_state.is_armed = False
            raise ConnectionError("synthetic commit acknowledgement loss")

    monkeypatch.setattr(ndc_publish, "handoff_ndc_stages", arm_lost_ack)
    monkeypatch.setattr(case.database, "transaction", lose_commit_ack)
    with pytest.raises(ConnectionError, match="acknowledgement"):
        await ndc_product.init_file({}, {"run_id": coordinator.run_id})
    run = await case.database.first(
        f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run_id", run_id=coordinator.run_id
    )
    receipt = run["metrics"]["ndc_handoff"]
    assert run["status"] == "finalizing" and run["finished_at"] is None
    assert ndc_handoff.has_valid_ndc_handoff(receipt, coordinator.run_id)
    assert all(state["row_count"] == 2 for state in (await _pair_state(case, "_" + receipt["attempt_id"])).values())
    assert not any(event["status"] in {"succeeded", "failed"} for event in coordinator.events)


async def _wait_for_stage_waiter(case):
    async with asyncio.timeout(2):
        while not await case.database.scalar("""
            SELECT EXISTS (SELECT 1 FROM pg_stat_activity
             WHERE datname=current_database() AND pid<>pg_backend_pid()
               AND wait_event_type='Lock' AND cardinality(pg_blocking_pids(pid))>0)
        """):
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["save", "cleanup"])
async def test_handoff_wins_waiting_worker_race(publication_case, monkeypatch, action):
    case = publication_case
    await _save_one(case)
    ready = asyncio.Event()
    release = asyncio.Event()
    original_handoff = ndc_publish.handoff_ndc_stages

    async def pause_handoff(*args):
        receipt = await original_handoff(*args)
        ready.set()
        await release.wait()
        return receipt

    monkeypatch.setattr(ndc_publish, "handoff_ndc_stages", pause_handoff)
    handoff_task = asyncio.create_task(_handoff(case))
    pending_tasks = [handoff_task]
    try:
        async with asyncio.timeout(8):
            await ready.wait()
            pending_tasks.append(
                asyncio.create_task(
                    _save_one(case, 2)
                    if action == "save"
                    else ndc_stage.discard_ndc_stages(case.database, case.attempt)
                )
            )
            await _wait_for_stage_waiter(case)
            release.set()
            outcomes = await asyncio.gather(*pending_tasks, return_exceptions=True)
        assert ndc_handoff.has_valid_ndc_handoff(outcomes[0], case.attempt.run_id)
        assert isinstance(outcomes[1], RuntimeError)
        assert (await _run_state(case))["status"] == "finalizing"
        assert all(state["row_count"] == 1 for state in (await _pair_state(case, "_" + case.attempt.suffix)).values())
    finally:
        release.set()
        for pending in pending_tasks:
            pending.cancel()
        await asyncio.gather(*pending_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_borrowed_transaction_cannot_advertise_handoff(publication_case):
    case = publication_case
    await _save_one(case)
    before = await _pair_state(case, "_" + case.attempt.suffix)
    async with case.database.transaction():
        with pytest.raises(RuntimeError, match="unbound database session"):
            await _handoff(case)
    assert await _pair_state(case, "_" + case.attempt.suffix) == before
    assert (await _run_state(case))["status"] == "running"


@pytest.mark.asyncio
async def test_external_connection_cannot_advertise_handoff(publication_case, monkeypatch):
    case = publication_case
    await _save_one(case)
    coordinator = await _coordinator_case(case, monkeypatch)
    before = await _pair_state(case, "_" + case.attempt.suffix)
    async with case.database.engine.connect() as independent, independent.begin():
        factory = async_sessionmaker(bind=independent, expire_on_commit=False, autoflush=False)
        with monkeypatch.context() as patch:
            patch.setattr(case.database, "session_factory", factory)
            patch.setattr(ndc_product, "ensure_import_run_table", AsyncMock())
            with pytest.raises(RuntimeError, match="own database engine binding"):
                await ndc_product.init_file({}, {"run_id": coordinator.run_id})
            ndc_product.ensure_import_run_table.assert_not_awaited()
            with pytest.raises(RuntimeError, match="own database engine binding"):
                await _handoff(case)
        assert independent.in_transaction()
    assert await _pair_state(case, "_" + case.attempt.suffix) == before
    assert (await _run_state(case))["status"] == "running"


@pytest.mark.asyncio
async def test_commit_delay_expires_before_announcement(publication_case, monkeypatch):
    case = publication_case
    coordinator = await _coordinator_case(case, monkeypatch)
    before = await _pair_state(case)
    original_transaction = case.database.transaction
    original_handoff = ndc_publish.handoff_ndc_stages
    delay_state = SimpleNamespace(is_armed=False, has_delayed=False)

    async def arm_commit_delay(*args):
        receipt = await original_handoff(*args)
        delay_state.is_armed = True
        return receipt

    @asynccontextmanager
    async def delay_commit():
        async with original_transaction() as session:
            yield session
            if delay_state.is_armed:
                delay_state.is_armed = False
                delay_state.has_delayed = True
                await asyncio.sleep(4)

    monkeypatch.setattr(ndc_publish, "handoff_ndc_stages", arm_commit_delay)
    monkeypatch.setattr(case.database, "transaction", delay_commit)
    async with asyncio.timeout(8):
        with pytest.raises(TimeoutError):
            await ndc_product.init_file({}, {"run_id": coordinator.run_id})
    assert delay_state.has_delayed
    run = await case.database.first(
        f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run_id", run_id=coordinator.run_id
    )
    assert run["status"] == "failed" and "ndc_handoff" not in run["metrics"]
    assert await ndc_stage.ndc_table_oids(case.database, case.schema, run["metrics"]["ndc_attempt_id"]) == {
        "product": None,
        "package": None,
    }
    assert await _pair_state(case) == before
    assert not any(event["status"] in {"succeeded", "finalizing"} for event in coordinator.events)


@pytest.mark.asyncio
async def test_reader_hands_off_beside_protected_views(publication_case, monkeypatch):
    case = publication_case
    async with handoff_reader_case(case, monkeypatch) as reader:
        identity = await reader.database.first("""
            SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls
              FROM pg_roles WHERE rolname=current_user
        """)
        assert identity["rolname"] == reader.reader_name
        assert not any(
            identity[key] for key in ("rolsuper", "rolcreaterole", "rolcreatedb", "rolreplication", "rolbypassrls")
        )
        assert (
            await reader.database.scalar(
                "SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)"
            )
            == 0
        )
        for statement in (
            f"ALTER VIEW {case.schema}.product RENAME TO forbidden_product",
            f"UPDATE {case.schema}.product SET brand_name='forbidden'",
            f"DROP VIEW {case.schema}.package",
            f"DROP TABLE {case.schema}.product_generation",
            f"UPDATE {case.schema}.product_generation SET brand_name='forbidden'",
        ):
            with pytest.raises(SQLAlchemyError) as denied:
                await reader.database.status(statement)
            assert denied.value.orig.sqlstate == "42501"
        coordinator = await _coordinator_case(reader, monkeypatch)
        before = await _pair_state(case, "_generation")
        view_oids = await ndc_stage.ndc_table_oids(reader.database, case.schema)
        assert await reader.database.scalar(f"SELECT count(*) FROM {case.schema}.product") == 1
        worker_result = await _run_worker(coordinator.run_id, monkeypatch)
        assert worker_result["status"] == "finalizing"
        assert ndc_handoff.has_valid_ndc_handoff(worker_result["result"], coordinator.run_id)
        assert await _pair_state(case, "_generation") == before
        assert await ndc_stage.ndc_table_oids(reader.database, case.schema) == view_oids
        assert worker_result["result"]["incumbent_oids"] == view_oids
        owners = await case.database.all(
            """
            SELECT c.relname, r.rolname FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner
             JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname=:schema AND c.relname IN ('product','package','product_generation','package_generation')
        """,
            schema=case.schema,
        )
        assert len(owners) == 4 and all(entry["rolname"] == reader.owner_name for entry in owners)
