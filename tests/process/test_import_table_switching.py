import asyncio
import hashlib
import importlib
import json
import os
import subprocess
import sys
import threading
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlsplit

import pytest
from sqlalchemy import Column, MetaData, String, Table, text

from api import control_imports, control_run_store, control_workers
from db.connection import Database
from process import control_lifecycle
from tests.process.test_result_publication_authority_postgres import _run_migration

control = importlib.import_module("api.control")
drug_indications = importlib.import_module("process.drug_indications")
label = importlib.import_module("process.label")
label_publish = importlib.import_module("process.label_publish")
ndc_product = importlib.import_module("process.ndc_product")
ndc_publish = importlib.import_module("process.ndc_publish")

_NDC_INDEXES = (
    ("product", "product_idx_product_ndc", "product_ndc"),
    ("product", "product_idx_brand_trgm_idx", "brand_name gin_trgm_ops"),
    ("product", "product_idx_generic_trgm_idx", "generic_name gin_trgm_ops"),
    ("product", "product_rxnorm_idx", "rxnorm_ids"),
    ("package", "package_idx_product_ndc", "product_ndc"),
)


async def _seed_ndc_generations(database, schema):
    for suffix, generation in (("", "current"), ("_old", "previous"), ("_20260909", "incoming")):
        await database.status(
            f"CREATE TABLE {schema}.product{suffix} (product_ndc text, brand_name text, "
            "generic_name text, rxnorm_ids text[], generation text)"
        )
        await database.status(f"CREATE TABLE {schema}.package{suffix} (product_ndc text, generation text)")
        await database.status(
            f"INSERT INTO {schema}.product{suffix} VALUES "
            "('00001-0001', 'Example', 'Example', ARRAY['1'], :generation)",
            generation=generation,
        )
        await database.status(
            f"INSERT INTO {schema}.package{suffix} VALUES ('00001-0001', :generation)",
            generation=generation,
        )
        if generation == "incoming":
            continue
        for table, index, columns in _NDC_INDEXES:
            await database.status(f"CREATE INDEX {index}{suffix} ON {schema}.{table}{suffix} USING GIN({columns})")


@pytest.fixture
async def ndc_publication_database():
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if os.getenv("HLTHPRT_ENVIRONMENT") != "test" or not ("test" in database_name or database_name.endswith("_ci")):
        pytest.skip("requires an explicitly configured disposable PostgreSQL test database")
    database = Database()
    schema = f"ndc_publish_test_{uuid.uuid4().hex}"
    is_schema_created = False
    try:
        await database.connect()
        for extension in ("pg_trgm", "btree_gin"):
            await database.status(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        await database.status(f"CREATE SCHEMA {schema}")
        is_schema_created = True
        await _seed_ndc_generations(database, schema)
        yield database, schema
    finally:
        try:
            if is_schema_created:
                await database.status(f"DROP SCHEMA {schema} CASCADE")
        finally:
            await database.disconnect()


async def _ndc_generation_state(database, schema, suffix):
    state_by_table = {}
    for table in ("product", "package"):
        relation = f"{schema}.{table}{suffix}"
        state_by_table[table] = {
            "oid": await database.scalar("SELECT CAST(:name AS regclass)::oid", name=relation),
            "generation": await database.scalar(f"SELECT generation FROM {relation}"),
            "indexes": tuple(
                (
                    await database.execute(
                        "SELECT c.relname, c.oid, i.indisvalid FROM pg_index i "
                        "JOIN pg_class c ON c.oid = i.indexrelid "
                        "WHERE i.indrelid = CAST(:name AS regclass) ORDER BY c.relname",
                        name=relation,
                    )
                ).all()
            ),
        }
    return state_by_table


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_package", [False, True], ids=["success", "package_failure"])
async def test_ndc_publication_preserves_generations_and_indexes(ndc_publication_database, monkeypatch, fail_package):
    database, schema = ndc_publication_database
    before_by_suffix = {
        suffix: await _ndc_generation_state(database, schema, suffix) for suffix in ("", "_old", "_20260909")
    }
    publish_single = ndc_publish._publish_single_ndc_table

    async def fail_after_product(database, db_schema, table, import_date):
        if table == "package":
            assert await database.scalar(f"SELECT generation FROM {schema}.product") == "incoming"
            raise RuntimeError("injected package publication failure")
        await publish_single(database, db_schema, table, import_date)

    if fail_package:
        monkeypatch.setattr(ndc_publish, "_publish_single_ndc_table", fail_after_product)
        with pytest.raises(RuntimeError, match="injected package publication failure"):
            await ndc_publish.publish_ndc_tables(database, schema, "20260909")
        assert await _ndc_generation_state(database, schema, "") == before_by_suffix[""]
        assert await _ndc_generation_state(database, schema, "_old") == before_by_suffix["_old"]
        assert await _ndc_generation_state(database, schema, "_20260909") == before_by_suffix["_20260909"]
        monkeypatch.setattr(ndc_publish, "_publish_single_ndc_table", publish_single)

    await ndc_publish.publish_ndc_tables(database, schema, "20260909")
    incoming = await _ndc_generation_state(database, schema, "")
    previous = await _ndc_generation_state(database, schema, "_old")
    for table in ("product", "package"):
        assert previous[table]["oid"] == before_by_suffix[""][table]["oid"]
        assert previous[table]["generation"] == "current"
        assert (
            tuple((name.removesuffix("_old"), oid, valid) for name, oid, valid in previous[table]["indexes"])
            == before_by_suffix[""][table]["indexes"]
        )
        assert await database.scalar("SELECT to_regclass(:name)", name=f"{schema}.{table}_20260909") is None
        removed_oids = [before_by_suffix["_old"][table]["oid"]] + [
            index_row[1] for index_row in before_by_suffix["_old"][table]["indexes"]
        ]
        for oid in removed_oids:
            assert not await database.scalar("SELECT 1 FROM pg_class WHERE oid = :oid", oid=oid)
        assert incoming[table]["oid"] == before_by_suffix["_20260909"][table]["oid"]
        assert incoming[table]["generation"] == "incoming"
        assert [index_row[0] for index_row in incoming[table]["indexes"]] == sorted(
            index for owner, index, _ in _NDC_INDEXES if owner == table
        )
        assert all(index_row[2] for index_row in incoming[table]["indexes"])


class _Func:
    def count(self, *_args, **_kwargs):
        return "count"


class _ScalarQuery:
    def __init__(self, values):
        self._values = values

    async def scalar(self):
        return self._values.pop(0)


class _RecordingTransaction:
    def __init__(self, db):
        self._db = db

    async def __aenter__(self):
        self._db.events.append(("begin", None))
        return self._db

    async def __aexit__(self, exc_type, _exc, _tb):
        self._db.events.append(("rollback" if exc_type else "commit", None))
        return False


class _RecordingDb:
    def __init__(self, scalar_values=()):
        self.created_tables = []
        self.events = []
        self.func = _Func()
        self.scalar_values = list(scalar_values)
        self.statements = []

    async def status(self, statement):
        self.statements.append(statement)
        self.events.append(("status", statement))

    async def scalar(self, statement, **parameters):
        assert "pg_try_advisory_xact_lock" in str(statement)
        assert parameters["scope"] == "rx_data:ndc-publication"
        self.events.append(("scalar", str(statement)))
        return self.scalar_values.pop(0)

    async def create_table(self, table, **_kwargs):
        self.created_tables.append(table)
        self.events.append(("create_table", table.name))

    def select(self, *_args, **_kwargs):
        return _ScalarQuery(self.scalar_values)

    def transaction(self):
        return _RecordingTransaction(self)


class _FakeSession:
    def __init__(self):
        self.closed = False

    def in_transaction(self):
        return False

    async def commit(self):
        return None

    async def rollback(self):
        return None

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_database_session_reuses_active_async_session():
    session = _FakeSession()
    db = Database()
    db.session_factory = lambda: session

    async with db.session() as outer:
        async with db.session() as inner:
            assert inner is outer

    assert session.closed is True


@pytest.mark.asyncio
async def test_ndc_startup_does_not_touch_stage_tables(monkeypatch):
    fake_db = _RecordingDb()

    async def fake_init_db(*_args, **_kwargs):
        return None

    monkeypatch.setattr(ndc_product, "db", fake_db)
    monkeypatch.setattr(ndc_product, "init_db", fake_init_db)
    await ndc_product.startup({})
    assert not fake_db.created_tables
    assert not fake_db.statements


@pytest.mark.asyncio
async def test_ndc_legacy_publication_indexes_stages_before_serving_changes_in_one_transaction():
    fake_db = _RecordingDb(scalar_values=[True])
    await ndc_publish.publish_ndc_tables(fake_db, "rx_data", "20260213")
    assert fake_db.events.count(("begin", None)) == 1
    assert fake_db.events.count(("commit", None)) == 1
    transaction_start = fake_db.events.index(("begin", None))
    index_positions = [
        index
        for index, (event, statement) in enumerate(fake_db.events)
        if event == "status" and statement.startswith("CREATE INDEX ")
    ]
    first_serving_change = fake_db.events.index(("status", "DROP TABLE IF EXISTS rx_data.product_old;"))
    assert len(index_positions) == 5
    assert transaction_start < min(index_positions) <= max(index_positions) < first_serving_change
    fence_position = next(index for index, (event, _) in enumerate(fake_db.events) if event == "scalar")
    assert max(index_positions) < fence_position < first_serving_change
    assert "ALTER TABLE IF EXISTS rx_data.product RENAME TO product_old;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.product_20260213 RENAME TO product;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.package RENAME TO package_old;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.package_20260213 RENAME TO package;" in fake_db.statements


@pytest.mark.asyncio
async def test_label_startup_preserves_existing_stages(monkeypatch):
    fake_db = _RecordingDb()

    async def fake_init_db(*_args, **_kwargs):
        return None

    monkeypatch.setattr(label, "db", fake_db)
    monkeypatch.setattr(label, "init_db", fake_init_db)

    context_dict = {}
    await label.label_startup(context_dict)

    assert context_dict == {}
    assert fake_db.created_tables == []
    assert fake_db.statements == []


@pytest.mark.asyncio
async def test_three_argument_label_publisher_keeps_its_transaction_interface(monkeypatch):
    fake_db = _RecordingDb()

    async def fake_mark_control_run(*_args, **_kwargs):
        return None

    monkeypatch.setattr(label, "db", fake_db)
    monkeypatch.setattr(label, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(label, "print_time_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("process.label_publish.publish_local_result_generation", fake_mark_control_run)
    monkeypatch.setattr("process.label_publish.require_label_ordinary_publication", fake_mark_control_run)
    monkeypatch.setattr("process.label_publish.read_result_publication_authority", fake_mark_control_run)

    await label_publish.publish_label_table(fake_db, "rx_data", "20260213")

    assert fake_db.events.count(("begin", None)) == 1
    assert fake_db.events.count(("commit", None)) == 1
    assert "ALTER TABLE IF EXISTS rx_data.label RENAME TO label_old;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.label_20260213 RENAME TO label;" in fake_db.statements


@pytest.mark.asyncio
async def test_drug_indications_publish_switches_staging_table(monkeypatch):
    fake_db = _RecordingDb()

    async def fake_publish(*_args, **_kwargs):
        return None

    monkeypatch.setattr(drug_indications, "db", fake_db)
    monkeypatch.setattr(drug_indications, "publish_local_result_generation", fake_publish)

    await drug_indications._publish(
        "rx_data",
        "20260213",
        {
            "format": "drug-result-consumed-dependencies-v1",
            "label": {"relations": [{"name": "label", "oid": 1}]},
            "ndc": {"relations": [{"name": "product", "oid": 2}]},
            "clinical-reference": {
                "relations": [
                    {"name": "code_relationship", "oid": 3},
                    {"name": "code_catalog", "oid": 4},
                    {"name": "code_synonym", "oid": 5},
                ]
            },
        },
    )

    assert "DROP TABLE IF EXISTS rx_data.drug_condition_evidence;" in fake_db.statements
    assert (
        "ALTER TABLE IF EXISTS rx_data.drug_condition_evidence_20260213 RENAME TO drug_condition_evidence;"
    ) in fake_db.statements


async def _seed_label_completion_case(database, schema, run_id, suffix):
    """Create a genuine serving generation, isolated stage and managed Label run."""
    await control_lifecycle.ensure_import_run_table()
    for name, count in (("label", 1), ("label_" + suffix, 3)):
        await database.status(
            f'CREATE TABLE "{schema}"."{name}" '
            "(id text PRIMARY KEY, set_id text, product_ndc text[], package_ndc text[])"
        )
        for index in range(count):
            await database.status(text(f'INSERT INTO "{schema}"."{name}" (id) VALUES (:id)'), id=str(index))
    async with database.engine.begin() as connection:
        await connection.run_sync(_run_migration)
    async with database.transaction():
        await label_publish.publish_local_result_generation(
            database, importer_id="label", schema=schema, consumed_dependencies={}
        )
    await database.status(
        text(
            f"INSERT INTO {schema}.import_run (run_id,engine,importer,node_id,status,metrics) "
            "VALUES (:run,'drug-api','label','example-node','running','{}'::jsonb)"
        ),
        run=run_id,
    )
    worker_context_dict = {"label_worker_claim_id": "example-claim"}
    await label_publish.claim_label_run(database, worker_context_dict, run_id)
    oid = await database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{schema}.label_{suffix}")
    context_dict = {
        "control_run_id": run_id,
        "import_date": suffix,
        "label_stage_oid": oid,
        "context": {"label_count": 3},
    }
    case = SimpleNamespace(
        database=database,
        schema=schema,
        run_id=run_id,
        suffix=suffix,
        context_dict=context_dict,
        stage_oid=oid,
        wrapper_context_dict=worker_context_dict,
    )
    case.before = await label_publish._label_state(database, schema)
    await database.status(
        text(
            f"UPDATE {schema}.import_run SET metrics=COALESCE(metrics,'{{}}'::jsonb) || CAST(:metrics AS jsonb) WHERE run_id=:run"
        ),
        run=run_id,
        metrics=json.dumps({"label_publication_intent": label_publish.label_publication_intent(context_dict, schema)}),
    )
    return case


@pytest.fixture
async def completion_case(monkeypatch):
    """Use only an explicitly selected task-owned local PostgreSQL test database."""
    name = os.getenv("HLTHPRT_DB_DATABASE", "")
    host = os.getenv("HLTHPRT_DB_HOST") or os.getenv("DB_HOST") or "127.0.0.1"
    if os.getenv("HLTHPRT_ENVIRONMENT") != "test" or not ("test" in name or name.endswith("_ci")):
        pytest.skip("requires an explicitly configured disposable PostgreSQL test database")
    if host not in {"127.0.0.1", "localhost"}:
        pytest.fail("Label completion tests require the task-owned local PostgreSQL host")
    database = Database()
    schema = "label_completion_" + uuid.uuid4().hex
    run_id = "example-run"
    suffix = "l" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
    monkeypatch.setenv("DB_SCHEMA", schema)
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "example-node")
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(label, "db", database)
    monkeypatch.setattr(control_lifecycle, "db", database)
    monkeypatch.setattr(control_workers, "db", database)
    for module in (label, control_lifecycle):
        monkeypatch.setattr(module, "enqueue_status_event", Mock())
        monkeypatch.setattr(module, "enqueue_live_progress", Mock())
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    await database.connect()
    try:
        case = await _seed_label_completion_case(database, schema, run_id, suffix)

        async def acquired(context, _task):
            context["label_stage_oid"] = case.stage_oid
            context["context"]["label_count"] = 3

        monkeypatch.setattr(label, "_acquire_label_source", acquired)
        yield case
    finally:
        try:
            await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await database.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "primary,fallback,allowed",
    [
        (None, "remote.example", False),
        ("", "remote.example", False),
        ("localhost", "remote.example", True),
        ("", "", True),
    ],
)
async def test_completion_fixture_checks_effective_database_host(monkeypatch, primary, fallback, allowed):
    if primary is None:
        monkeypatch.delenv("HLTHPRT_DB_HOST", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_DB_HOST", primary)
    monkeypatch.setenv("DB_HOST", fallback)
    monkeypatch.setenv("HLTHPRT_ENVIRONMENT", "test")
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "disposable_test")
    constructor = Mock(side_effect=RuntimeError("host accepted before any connection"))
    monkeypatch.setattr(sys.modules[__name__], "Database", constructor)
    fixture = completion_case.__wrapped__(monkeypatch)
    try:
        error = RuntimeError if allowed else pytest.fail.Exception
        with pytest.raises(error, match="host accepted" if allowed else "local PostgreSQL host"):
            await anext(fixture)
        assert constructor.call_count == int(allowed)
    finally:
        await fixture.aclose()


async def _run_row(case):
    return await case.database.first(
        text(f"SELECT status, metrics FROM {case.schema}.import_run WHERE run_id=:run"), run=case.run_id
    )


def _commit_fault(case, monkeypatch, *, committed, canceled=False, unavailable=False, cancel_status=None):
    """Inject a transport boundary after a real commit or a real rollback."""
    original = case.database.transaction
    depths = [0]

    @asynccontextmanager
    async def transaction():
        depths[0] += 1
        is_outer = depths[0] == 1
        try:
            async with original() as session:
                yield session
                if is_outer and not committed:
                    raise asyncio.CancelledError() if canceled else RuntimeError("commit response lost")
            if is_outer:
                if unavailable:
                    case.database.session_factory = Mock(side_effect=RuntimeError("reconciliation unavailable"))
                raise asyncio.CancelledError() if canceled else RuntimeError("commit response lost")
        except BaseException:
            if is_outer and cancel_status is not None:
                await case.database.status(
                    text(f"UPDATE {case.schema}.import_run SET status=:status"), status=cancel_status
                )
            raise
        finally:
            depths[0] -= 1

    monkeypatch.setattr(case.database, "transaction", transaction)


@pytest.mark.asyncio
async def test_label_success_commits_heap_generation_and_run_receipt_together(completion_case):
    case = completion_case
    receipt = await label_publish.complete_label_publication(case.database, case.context_dict)
    row = await _run_row(case)
    state = await label_publish._label_state(case.database, case.schema)
    assert row["status"] == "succeeded" and row["metrics"]["label_publication"] == receipt
    assert state == {"table_oid": case.stage_oid, "generation": receipt["generation"]}
    assert state["generation"]["local_generation"] == case.before["generation"]["local_generation"] + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["failed", "canceling", "node"])
async def test_lost_managed_cas_rolls_back_heap_and_generation(completion_case, monkeypatch, damage):
    case = completion_case
    if damage == "node":
        monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "other-node")
    else:
        await case.database.status(text(f"UPDATE {case.schema}.import_run SET status=:status"), status=damage)
    expected = asyncio.CancelledError if damage == "canceling" else RuntimeError
    with pytest.raises(expected):
        await label.init_label_file(case.wrapper_context_dict, {"run_id": case.run_id})
    assert await label_publish._label_state(case.database, case.schema) == case.before
    assert (
        await case.database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{case.schema}.label_{case.suffix}")
        == case.stage_oid
    )
    row = await _run_row(case)
    assert row["status"] == ("canceled" if damage == "canceling" else "running" if damage == "node" else "failed")


@pytest.mark.asyncio
@pytest.mark.parametrize("canceled", [False, True])
@pytest.mark.parametrize("managed", [False, True])
async def test_lost_commit_response_recovers_genuine_committed_success(completion_case, monkeypatch, canceled, managed):
    case = completion_case
    _commit_fault(case, monkeypatch, committed=True, canceled=canceled)
    if managed:
        receipt = await label.init_label_file(case.wrapper_context_dict, {"run_id": case.run_id})
        assert (await _run_row(case))["metrics"]["label_publication"] == receipt
    else:
        case.context_dict["control_run_id"] = None
        receipt = await label_publish.complete_label_publication(case.database, case.context_dict)
    assert await label_publish._label_state(case.database, case.schema) == {
        "table_oid": case.stage_oid,
        "generation": receipt["generation"],
    }


@pytest.mark.asyncio
async def test_cancellation_before_commit_retains_stage_and_projects_canceled(completion_case, monkeypatch):
    case = completion_case
    _commit_fault(case, monkeypatch, committed=False, canceled=True)
    with pytest.raises(asyncio.CancelledError):
        await label.init_label_file(case.wrapper_context_dict, {"run_id": case.run_id})
    assert (await _run_row(case))["status"] == "canceled"
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_unknown_commit_never_projects_generic_failure(completion_case, monkeypatch, committed):
    case = completion_case
    factory = case.database.session_factory
    _commit_fault(case, monkeypatch, committed=committed, unavailable=True)
    if not committed:
        original_reconcile = label_publish._await_label_reconciliation
        monkeypatch.setattr(
            label_publish,
            "_await_label_reconciliation",
            AsyncMock(side_effect=RuntimeError("reconciliation unavailable")),
        )
    try:
        payload_dict = {
            "run_id": case.run_id,
            "importer": "label",
            "target_module": "process.label",
            "target_function": "init_label_file",
            "call_style": "ctx_task",
            "task": {},
        }
        with pytest.raises(RuntimeError, match="reconciliation unavailable"):
            await control_lifecycle.control_single_job_start(case.wrapper_context_dict, payload_dict)
        assert case.wrapper_context_dict["label_commit_unknown"] is True
    finally:
        case.database.session_factory = factory
        if not committed:
            monkeypatch.setattr(label_publish, "_await_label_reconciliation", original_reconcile)
    assert (await _run_row(case))["status"] == ("succeeded" if committed else "running")
    observed = await label_publish._label_state(case.database, case.schema)
    assert observed["table_oid"] == (case.stage_oid if committed else case.before["table_oid"])
    assert observed["generation"]["local_generation"] == case.before["generation"]["local_generation"] + int(committed)


@pytest.mark.asyncio
@pytest.mark.parametrize("canceled", [False, True])
async def test_postcommit_projection_failure_preserves_managed_success(completion_case, monkeypatch, canceled):
    case = completion_case
    failure = asyncio.CancelledError() if canceled else RuntimeError("event unavailable")
    monkeypatch.setattr(label, "enqueue_status_event", Mock(side_effect=failure))
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock(side_effect=failure))
    payload_dict = {
        "run_id": case.run_id,
        "importer": "label",
        "target_module": "process.label",
        "target_function": "init_label_file",
        "call_style": "ctx_task",
        "task": {},
    }
    result = await control_lifecycle.control_single_job_start(case.wrapper_context_dict, payload_dict)
    assert result["status"] == "succeeded" and (await _run_row(case))["status"] == "succeeded"


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_reconciliation_waits_for_the_original_transaction_row_lock(completion_case, monkeypatch, managed):
    case = completion_case
    if not managed:
        case.context_dict["control_run_id"] = None
    original = case.database.transaction
    ready, release = asyncio.Event(), asyncio.Event()
    depths = [0]

    @asynccontextmanager
    async def hold_commit():
        depths[0] += 1
        try:
            async with original() as session:
                yield session
                if depths[0] == 1:
                    ready.set()
                    await release.wait()
        finally:
            depths[0] -= 1

    monkeypatch.setattr(case.database, "transaction", hold_commit)
    publisher = asyncio.create_task(label_publish.complete_label_publication(case.database, case.context_dict))
    reader = None
    try:
        await asyncio.wait_for(ready.wait(), 5)
        reader = asyncio.create_task(
            label_publish._reconcile_label_publication(
                case.database,
                case.context_dict,
                case.schema,
                case.suffix,
                case.context_dict["label_publication_candidate"],
            )
        )
        await asyncio.sleep(0.05)
        assert not reader.done()
        release.set()
        await publisher
        assert await reader == "succeeded"
    finally:
        release.set()
        await asyncio.gather(publisher, *([reader] if reader else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_commit_error_with_reconciled_cancellation_finishes_canceled(completion_case, monkeypatch):
    case = completion_case
    _commit_fault(case, monkeypatch, committed=False, cancel_status="canceling")
    with pytest.raises(asyncio.CancelledError):
        await label.init_label_file(case.wrapper_context_dict, {"run_id": case.run_id})
    assert (await _run_row(case))["status"] == "canceled"
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
async def test_cancellation_during_shielded_reconcile_is_retained_after_rollback(completion_case, monkeypatch):
    case = completion_case
    original = label_publish._reconcile_label_publication
    started, release = asyncio.Event(), asyncio.Event()

    async def delayed(*args):
        started.set()
        await release.wait()
        return await original(*args)

    _commit_fault(case, monkeypatch, committed=False)
    monkeypatch.setattr(label_publish, "_reconcile_label_publication", delayed)
    worker = asyncio.create_task(label.init_label_file(case.wrapper_context_dict, {"run_id": case.run_id}))
    try:
        await asyncio.wait_for(started.wait(), 5)
        worker.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
    finally:
        release.set()
        await asyncio.gather(worker, return_exceptions=True)
    assert (await _run_row(case))["status"] == "canceled"
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["finished", "error"])
async def test_reconciliation_refuses_damaged_terminal_metadata(completion_case, damage):
    case = completion_case
    receipt = await label_publish.complete_label_publication(case.database, case.context_dict)
    if damage == "finished":
        await case.database.status(f"UPDATE {case.schema}.import_run SET finished_at=NULL")
    else:
        await case.database.status(f'UPDATE {case.schema}.import_run SET error=\'{{"code":"test"}}\'::jsonb')
    with pytest.raises(RuntimeError, match="outcome could not be confirmed"):
        await label_publish._reconcile_label_publication(
            case.database, case.context_dict, case.schema, case.suffix, receipt
        )
    assert (await _run_row(case))["status"] == "succeeded"


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["complete", "coordinator", "control"])
async def test_label_completion_refuses_ambient_transaction_before_writes(completion_case, entry):
    case = completion_case
    before_run_dict = dict(await _run_row(case))
    before_context_dict = dict(case.wrapper_context_dict)
    async with case.database.transaction():
        with pytest.raises(RuntimeError, match="top-level transaction"):
            if entry == "complete":
                await label_publish.complete_label_publication(case.database, case.context_dict)
            elif entry == "coordinator":
                await label.init_label_file(case.wrapper_context_dict, {"run_id": case.run_id})
            else:
                payload_dict = {
                    "run_id": case.run_id,
                    "target_module": "process.label",
                    "target_function": "init_label_file",
                    "task": {},
                }
                await control_lifecycle.control_single_job_start(case.wrapper_context_dict, payload_dict)
    assert dict(await _run_row(case)) == before_run_dict
    assert await label_publish._label_state(case.database, case.schema) == case.before
    assert case.wrapper_context_dict == before_context_dict


@pytest.mark.asyncio
async def test_three_argument_label_publisher_preserves_outer_rollback(completion_case):
    case = completion_case
    with pytest.raises(RuntimeError, match="outer rollback"):
        async with case.database.transaction():
            assert await label_publish.publish_label_table(case.database, case.schema, case.suffix) is None
            assert (await label_publish._label_state(case.database, case.schema))["table_oid"] == case.stage_oid
            raise RuntimeError("outer rollback")
    assert await label_publish._label_state(case.database, case.schema) == case.before
    assert (await _run_row(case))["status"] == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_managed_recovery_uses_durable_identity_after_unknown_commit(completion_case, monkeypatch, committed):
    case = completion_case
    factory = case.database.session_factory
    with monkeypatch.context() as fault:
        _commit_fault(case, fault, committed=committed, unavailable=True)
        if not committed:
            fault.setattr(
                label_publish,
                "_await_label_reconciliation",
                AsyncMock(side_effect=RuntimeError("reconciliation unavailable")),
            )
        try:
            with pytest.raises(RuntimeError, match="reconciliation unavailable"):
                await label.init_label_file(case.wrapper_context_dict, {"run_id": case.run_id})
        finally:
            case.database.session_factory = factory
    assert case.wrapper_context_dict["label_commit_unknown"] is True
    case.wrapper_context_dict.clear()
    reconciled_run_dict = await label_publish.reconcile_managed_label_run(case.database, case.run_id)
    assert reconciled_run_dict["status"] == ("succeeded" if committed else "failed")
    assert reconciled_run_dict["publication"] == ("committed" if committed else "unpublished")
    assert reconciled_run_dict["stage_oid"] == case.stage_oid
    if not committed:
        assert await label_publish._label_state(case.database, case.schema) == case.before
        assert (
            await case.database.scalar(text("SELECT to_regclass(:name)::oid"), name=reconciled_run_dict["stage"])
            == case.stage_oid
        )
        with pytest.raises(RuntimeError, match="active run ownership fence"):
            await label.init_label_file(case.wrapper_context_dict, {"run_id": case.run_id})
        assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "oid", "attempt", "node"])
async def test_managed_recovery_refuses_changed_retained_identity(completion_case, damage):
    case = completion_case
    intent = label_publish.label_publication_intent(case.context_dict, case.schema)
    if damage == "oid":
        intent["stage_oid"] += 1
    elif damage == "attempt":
        intent["attempt_id"] = "lwrong"
    elif damage == "node":
        intent["node_id"] = "other-node"
    metrics = {} if damage == "missing" else {"label_publication_intent": intent}
    await case.database.status(
        text(f"UPDATE {case.schema}.import_run SET metrics=CAST(:metrics AS jsonb)"), metrics=json.dumps(metrics)
    )
    before_run_dict = dict(await _run_row(case))
    with pytest.raises(RuntimeError, match="retained attempt identity|outcome remains unconfirmed"):
        await label_publish.reconcile_managed_label_run(case.database, case.run_id)
    assert dict(await _run_row(case)) == before_run_dict
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["coordinator", "control"])
async def test_label_coordinator_refuses_idle_bound_session_before_writes(completion_case, entry):
    case = completion_case
    before_run_dict = dict(await _run_row(case))
    async with case.database.session() as session:
        assert not session.in_transaction()
        with pytest.raises(RuntimeError, match="top-level transaction"):
            if entry == "coordinator":
                await label.init_label_file(case.wrapper_context_dict, {"run_id": case.run_id})
            else:
                payload_dict = {
                    "run_id": case.run_id,
                    "target_module": "process.label",
                    "target_function": "init_label_file",
                    "task": {},
                }
                await control_lifecycle.control_single_job_start(case.wrapper_context_dict, payload_dict)
        assert not session.in_transaction()
    assert dict(await _run_row(case)) == before_run_dict
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
async def test_acquisition_persists_and_confirms_managed_intent_before_download(completion_case):
    case = completion_case
    before_context_dict = dict(case.wrapper_context_dict)
    recorded_run, projection = await label._record_label_acquisition(case.context_dict, case.schema, 2)
    assert recorded_run == case.run_id and projection is not None
    run_row_dict = await case.database.first(
        text(f"SELECT status,phase_detail,metrics FROM {case.schema}.import_run WHERE run_id=:run"), run=case.run_id
    )
    assert run_row_dict["status"] == "running"
    assert run_row_dict["phase_detail"] == "label partitions selected"
    assert run_row_dict["metrics"]["label_publication_intent"] == label_publish.label_publication_intent(
        case.context_dict, case.schema
    )
    assert run_row_dict["metrics"]["partition_count"] == 2
    assert case.wrapper_context_dict == before_context_dict


async def _admit_label_run(case, monkeypatch, run_id):
    """Exercise actual admission while replacing only queue and projection I/O."""
    monkeypatch.setattr(control_imports, "db", case.database)
    monkeypatch.setattr(control_run_store, "db", case.database)
    monkeypatch.setattr(control_imports, "enqueue_status_event", Mock())
    monkeypatch.setattr(control_imports, "_write_run_live_progress", Mock())
    monkeypatch.setattr(
        control_imports,
        "_enqueue",
        AsyncMock(
            return_value={
                "status": "queued",
                "phase_detail": "enqueued",
                "heartbeat_at": control_imports.utc_now(),
                "progress": {},
                "metrics": {},
                "error": None,
            }
        ),
    )
    admitted_run_dict, created = await control_imports.create_import_run({"run_id": run_id, "importer": "label"})
    assert created and admitted_run_dict["run_id"] == run_id
    return admitted_run_dict


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_recovery_preserves_accepted_identity_contract(completion_case, monkeypatch, published):
    case = completion_case
    run_id = "label-" + "x" * 123
    admitted_run_dict = await _admit_label_run(case, monkeypatch, run_id)
    assert len(admitted_run_dict["run_id"]) == 129
    assert admitted_run_dict["node_id"] == "example-node"
    suffix = "l" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
    stage = f'"{case.schema}"."label_{suffix}"'
    await case.database.status(f'CREATE TABLE {stage} (LIKE "{case.schema}"."label_{case.suffix}" INCLUDING ALL)')
    await case.database.status(f'INSERT INTO {stage} SELECT * FROM "{case.schema}"."label_{case.suffix}"')
    stage_oid = await case.database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{case.schema}.label_{suffix}")
    context_dict = {
        "control_run_id": run_id,
        "import_date": suffix,
        "label_stage_oid": stage_oid,
        "context": {"label_count": 3},
    }
    monkeypatch.delenv("HLTHPRT_IMPORT_NODE_ID")
    await label._record_label_acquisition(context_dict, case.schema, 1)
    if published:
        receipt = await label_publish.complete_label_publication(case.database, context_dict)
        assert receipt["node_id"] is None and receipt["run_id"] == run_id
    context_dict.clear()
    before = await label_publish._label_state(case.database, case.schema)
    reconciled_run_dict = await label_publish.reconcile_managed_label_run(case.database, run_id)
    assert reconciled_run_dict["status"] == ("succeeded" if published else "failed")
    assert reconciled_run_dict["stage_oid"] == stage_oid
    assert await label_publish._label_state(case.database, case.schema) == before
    assert await case.database.scalar(text("SELECT to_regclass(:name)::oid"), name=reconciled_run_dict["stage"]) == (
        None if published else stage_oid
    )


def _bind_ended_label_worker(monkeypatch, run_id):
    image = "ghcr.io/example/worker@sha256:" + "1" * 64
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", image)
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "test-workers")
    job = control_workers.worker_job_manifest(control_workers._BY_IMPORTER["label"], {"run_id": run_id}, image)
    job["metadata"]["uid"] = "example-worker-uid"
    job["metadata"]["creationTimestamp"] = "2026-01-01T00:00:00Z"
    job["status"] = {"active": 0, "failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}
    monkeypatch.setenv("HLTHPRT_LABEL_WORKER_JOB_NAME", job["metadata"]["name"])
    monkeypatch.setenv("HLTHPRT_LABEL_WORKER_JOB_UID", job["metadata"]["uid"])
    monkeypatch.setattr(control_workers, "_kubernetes_request", Mock(return_value=job))
    return job


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "before-intent", "after-intent", "cancel", "commit-response"])
async def test_stage_and_intent_commit_atomically(completion_case, monkeypatch, fault):
    case = completion_case
    run_id = "stage-atomic-run"
    admitted_run_dict = await _admit_label_run(case, monkeypatch, run_id)
    suffix = "l" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
    table = Table("label_" + suffix, MetaData(), Column("id", String, primary_key=True), schema=case.schema)
    monkeypatch.setattr(
        label, "make_class", Mock(return_value=SimpleNamespace(__table__=table, __tablename__=table.name))
    )
    context_dict = {"control_run_id": run_id, "import_date": suffix, "context": {"label_count": 3}}
    original_record = label._record_label_acquisition
    original_transaction = case.database.transaction

    async def interrupted(*arguments):
        if fault != "before-intent":
            await original_record(*arguments)
        raise asyncio.CancelledError() if fault == "cancel" else RuntimeError("intent interrupted")

    @asynccontextmanager
    async def uncertain_commit():
        async with original_transaction() as session:
            yield session
        raise RuntimeError("commit response lost")

    if fault == "commit-response":
        monkeypatch.setattr(case.database, "transaction", uncertain_commit)
    elif fault:
        monkeypatch.setattr(label, "_record_label_acquisition", interrupted)
    if fault:
        with pytest.raises(asyncio.CancelledError if fault == "cancel" else RuntimeError):
            await label._prepare_label_stage(context_dict, case.schema, 1)
    else:
        assert await label._prepare_label_stage(context_dict, case.schema, 1) == run_id
    run_row_dict = await case.database.first(
        text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id
    )
    oid = await case.database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{case.schema}.{table.name}")
    if fault in {None, "commit-response"}:
        assert (
            oid == run_row_dict["metrics"]["label_publication_intent"]["stage_oid"] == context_dict["label_stage_oid"]
        )
        assert run_row_dict["status"] == "running"
    else:
        assert (
            oid is None
            and run_row_dict["metrics"] == admitted_run_dict["metrics"]
            and run_row_dict["status"] == "queued"
        )
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "old", "stage", "worker", "condition", "image", "cas"])
async def test_pre_stage_recovery_requires_closed_evidence(completion_case, monkeypatch, damage):
    case = completion_case
    run_id = "pre-stage-run"
    admitted_run_dict = await _admit_label_run(case, monkeypatch, run_id)
    job = _bind_ended_label_worker(monkeypatch, run_id)
    await label_publish.claim_label_run(case.database, {"job_id": "actual-message"}, run_id)
    protocol = admitted_run_dict["metrics"]["label_publication_protocol"]
    stage = f"{case.schema}.label_{protocol['attempt_id']}"
    if damage == "old":
        await case.database.status(
            text(f"UPDATE {case.schema}.import_run SET metrics='{{}}' WHERE run_id=:run"), run=run_id
        )
    if damage == "stage":
        await case.database.status(f"CREATE TABLE {stage} (id text)")
    if damage == "worker":
        job["metadata"]["uid"] = "other-worker-uid"
    if damage == "condition":
        job["status"]["conditions"] = [{"type": "FailureTarget", "status": "True"}]
    if damage == "image":
        job["spec"]["template"]["spec"]["containers"][0]["image"] = "ghcr.io/example/worker:old"
    if damage == "cas":
        status = case.database.status

        async def changed(statement, **parameters):
            return 0 if "identity_field" in parameters else await status(statement, **parameters)

        monkeypatch.setattr(case.database, "status", changed)
    before_run_dict = dict(
        await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
    )
    if damage:
        with pytest.raises(RuntimeError):
            await label_publish.reconcile_managed_label_run(case.database, run_id)
        assert (
            dict(
                await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
            )
            == before_run_dict
        )
    else:
        reconciled_run_dict = await label_publish.reconcile_managed_label_run(case.database, run_id)
        assert reconciled_run_dict == {
            "run_id": run_id,
            "stage": stage,
            "stage_oid": None,
            "status": "failed",
            "publication": "unpublished",
        }
    assert await label_publish._label_state(case.database, case.schema) == case.before
    assert (await case.database.scalar(text("SELECT to_regclass(:name)::oid"), name=stage) is not None) == (
        damage == "stage"
    )


@pytest.mark.asyncio
async def test_worker_exit_rolls_back_unrecorded_stage(completion_case, monkeypatch):
    case = completion_case
    run_id = "interrupted-stage-run"
    admitted_run_dict = await _admit_label_run(case, monkeypatch, run_id)
    protocol = admitted_run_dict["metrics"]["label_publication_protocol"]
    job = _bind_ended_label_worker(monkeypatch, run_id)
    job["status"] = {"active": 1, "conditions": []}
    context_dict = {"control_run_id": run_id, "import_date": protocol["attempt_id"], "context": {"label_count": 3}}
    script = """
import asyncio,importlib,json,os,sys
label=importlib.import_module('process.label')
publication=importlib.import_module('process.label_publish')
async def exit_before_intent(*_arguments):
    os._exit(17)
async def run():
    label._record_label_acquisition=exit_before_intent
    context=json.loads(sys.argv[1])
    await publication.claim_label_run(label.db,{'job_id':'actual-child-message'},context['control_run_id'])
    await label._prepare_label_stage(context,sys.argv[2],1)
asyncio.run(run())
"""
    child = subprocess.run(
        [sys.executable, "-c", script, json.dumps(context_dict), case.schema],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert child.returncode == 17, child.stderr
    assert (
        await case.database.scalar(
            text("SELECT to_regclass(:name)::oid"), name=f"{case.schema}.label_{protocol['attempt_id']}"
        )
        is None
    )
    job["status"] = {"active": 0, "failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}
    reconciled_run_dict = await label_publish.reconcile_managed_label_run(case.database, run_id)
    assert reconciled_run_dict["status"] == "failed" and reconciled_run_dict["stage_oid"] is None
    assert await label_publish._label_state(case.database, case.schema) == case.before


def _label_control_payload(run_id):
    return {
        "run_id": run_id,
        "importer": "label",
        "target_module": "process.label",
        "target_function": "init_label_file",
        "task": {},
    }


@pytest.mark.asyncio
async def test_recovery_follows_actual_shared_queue_consumer(completion_case, monkeypatch):
    case = completion_case
    run_id = "consumed-run-b"
    await _admit_label_run(case, monkeypatch, run_id)
    actual_job = _bind_ended_label_worker(monkeypatch, "launch-run-a")
    actual_job["status"] = {"active": 1, "conditions": []}
    unrelated_job = control_workers.worker_job_manifest(
        control_workers._BY_IMPORTER["label"], {"run_id": run_id}, "unrelated"
    )
    unrelated_job.update(status={"active": 0, "succeeded": 1, "conditions": [{"type": "Complete", "status": "True"}]})
    reads = Mock(
        side_effect=lambda _method, path: (
            actual_job if path.endswith("/" + actual_job["metadata"]["name"]) else {"items": [unrelated_job]}
        )
    )
    monkeypatch.setattr(control_workers, "_kubernetes_request", reads)
    started, release = asyncio.Event(), asyncio.Event()

    async def acquiring(_context, _task):
        started.set()
        await release.wait()
        raise RuntimeError("worker ended before stage")

    monkeypatch.setattr(label, "_acquire_label_source", acquiring)
    worker = asyncio.create_task(
        control_lifecycle.control_single_job_start({"job_id": "actual-message-b"}, _label_control_payload(run_id))
    )
    try:
        await asyncio.wait_for(started.wait(), 5)
        before_run_dict = dict(
            await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
        )
        claim = before_run_dict["metrics"]["label_worker_claim"]
        assert claim["arq_job_id"] == "actual-message-b" and claim["job_uid"] == actual_job["metadata"]["uid"]
        with pytest.raises(RuntimeError, match="exact terminal worker evidence"):
            await label_publish.reconcile_managed_label_run(case.database, run_id)
        with pytest.raises(RuntimeError, match="active run ownership fence"):
            await control_lifecycle.control_single_job_start(
                {"job_id": "competing-message"}, _label_control_payload(run_id)
            )
        assert (
            dict(
                await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
            )
            == before_run_dict
        )
        assert reads.call_args.args[1].endswith("/" + actual_job["metadata"]["name"])
    finally:
        release.set()
        outcomes = await asyncio.gather(worker, return_exceptions=True)
    assert isinstance(outcomes[0], RuntimeError) and str(outcomes[0]) == "worker ended before stage"
    actual_job["status"] = {"active": 0, "failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}
    recovered_run_dict = await label_publish.reconcile_managed_label_run(case.database, run_id)
    assert recovered_run_dict["status"] == "failed" and recovered_run_dict["stage_oid"] is None
    assert actual_job["metadata"]["annotations"]["healthporta.com/run-id"] != run_id
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "failed", "canceling"])
async def test_terminal_consumer_survives_until_distinct_retry(completion_case, monkeypatch, status):
    case = completion_case
    run_id = "claimed-run-b"
    await _admit_label_run(case, monkeypatch, run_id)
    actual_job = _bind_ended_label_worker(monkeypatch, "launch-run-a")
    await label_publish.claim_label_run(case.database, {"job_id": "message-b"}, run_id)
    await case.database.status(
        text(f"UPDATE {case.schema}.import_run SET status=:status WHERE run_id=:run"), status=status, run=run_id
    )
    requests = _retained_worker_requests(monkeypatch, actual_job)
    before_run_dict = dict(
        await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
    )

    ensured_dict = control_workers.ensure_worker({"importer": "label", "run_id": "launch-run-a"})

    assert ensured_dict["status"] == "exists"
    assert ensured_dict["items"][0]["job_uid"] == actual_job["metadata"]["uid"]
    assert all(call.args[0] == "GET" for call in requests.call_args_list)
    assert (
        dict(await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id))
        == before_run_dict
    )
    recovered_dict = await label_publish.reconcile_managed_label_run(case.database, run_id)
    assert recovered_dict["status"] == ("canceled" if status == "canceling" else "failed")
    assert recovered_dict["stage_oid"] is None
    recovered_run_dict = dict(
        await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
    )

    retry_dict, created = await control_imports.retry_import_run(run_id, {})
    fresh_worker_dict = control_workers.ensure_worker({"importer": "label", "run_id": retry_dict["run_id"]})

    assert created and retry_dict["run_id"] != run_id
    assert retry_dict["retry_of_run_id"] == run_id and fresh_worker_dict["status"] == "started"
    assert all(call.args[0] != "DELETE" for call in requests.call_args_list)
    assert (
        dict(await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id))
        == recovered_run_dict
    )
    assert actual_job["metadata"]["uid"] == before_run_dict["metrics"]["label_worker_claim"]["job_uid"]
    assert await label_publish._label_state(case.database, case.schema) == case.before


def _retained_worker_requests(monkeypatch, actual_job, *, pending=False):
    """Keep real Job manifests available while simulating only Kubernetes transport."""
    jobs_dict = {actual_job["metadata"]["name"]: actual_job}

    def request(method, path, body=None):
        if method == "DELETE":
            pytest.fail("terminal consumer evidence was deleted")
        if method == "POST":
            if body["metadata"]["name"] in jobs_dict:
                raise control_workers._KubernetesApiError(409, "already exists")
            body["metadata"].update(
                uid=f"new-worker-uid-{len(jobs_dict)}", creationTimestamp=f"2026-01-01T00:00:0{len(jobs_dict)}Z"
            )
            body["status"] = {} if pending else {"active": 1}
            jobs_dict[body["metadata"]["name"]] = body
            return body
        if "/jobs/" in path:
            return jobs_dict[path.rsplit("/", 1)[1]]
        selector = parse_qs(urlsplit(path).query)["labelSelector"][0]
        labels_dict = dict(pair.split("=", 1) for pair in selector.split(","))
        return {
            "items": [
                job
                for job in reversed(list(jobs_dict.values()))
                if all(job["metadata"]["labels"].get(key) == value for key, value in labels_dict.items())
            ]
        }

    requests = Mock(side_effect=request)
    monkeypatch.setattr(control_workers, "_kubernetes_request", requests)
    return requests


async def _ensure_label_route(monkeypatch, run_id):
    monkeypatch.setattr(control, "_require_control_auth", lambda _request: None)
    response = await control.control_ensure_worker(SimpleNamespace(json={"importer": "label", "run_id": run_id}))
    assert response.status == 202
    return json.loads(response.body)


@pytest.mark.asyncio
async def test_startup_repair_serializes_unclaimed_run(completion_case, monkeypatch):
    case = completion_case
    run_id = "unclaimed-startup-run"
    await _admit_label_run(case, monkeypatch, run_id)
    original_job = _bind_ended_label_worker(monkeypatch, run_id)
    original_job["metadata"]["uid"] = "worker-uid-41"
    requests = _retained_worker_requests(monkeypatch, original_job, pending=True)
    before_run_dict = dict(
        await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
    )

    results_list = await asyncio.gather(*(_ensure_label_route(monkeypatch, run_id) for _ in range(2)))

    assert sorted(result_dict["status"] for result_dict in results_list) == ["exists", "started"]
    posts_list = [call for call in requests.call_args_list if call.args[0] == "POST"]
    assert len(posts_list) == 1
    replacement_job = posts_list[0].args[2]
    assert replacement_job["metadata"]["name"] != original_job["metadata"]["name"]
    assert replacement_job["metadata"]["annotations"]["healthporta.com/run-id"] == run_id
    replacement_job["metadata"]["creationTimestamp"] = original_job["metadata"]["creationTimestamp"]
    assert replacement_job["metadata"]["name"] < original_job["metadata"]["name"]
    replacement_job["status"] = {"failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}

    assert (await _ensure_label_route(monkeypatch, run_id))["status"] == "started"

    posts_list = [call for call in requests.call_args_list if call.args[0] == "POST"]
    assert len(posts_list) == 2
    assert posts_list[1].args[2]["metadata"]["name"] == control_workers.worker_job_name(
        control_workers._BY_IMPORTER["label"], {"run_id": run_id}, previous_job_uid=replacement_job["metadata"]["uid"]
    )
    assert all(call.args[0] != "DELETE" for call in requests.call_args_list)
    assert original_job["metadata"]["uid"] == "worker-uid-41"
    assert (
        dict(await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id))
        == before_run_dict
    )
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "claimed",
        "terminal",
        "node",
        "legacy",
        "receipt",
        "foreign-engine",
        "started_at",
        "finished_at",
        "error",
        "done",
        "pct",
    ],
)
async def test_startup_repair_refuses_owned_native_run(completion_case, monkeypatch, damage):
    case = completion_case
    run_id = "unclaimed-startup-run"
    await _admit_label_run(case, monkeypatch, run_id)
    original_job = _bind_ended_label_worker(monkeypatch, run_id)
    requests = _retained_worker_requests(monkeypatch, original_job)
    assignments_dict = {
        "claimed": "metrics=metrics || CAST(:marker AS jsonb)",
        "terminal": "status='failed'",
        "node": "node_id='foreign-node'",
        "legacy": "metrics='{}'::jsonb",
        "receipt": "metrics=metrics || CAST(:marker AS jsonb)",
        "foreign-engine": "engine='foreign'",
        "started_at": "started_at=CURRENT_TIMESTAMP",
        "finished_at": "finished_at=CURRENT_TIMESTAMP",
        "error": "error=CAST(:detail AS jsonb)",
        "done": "progress=CAST(:detail AS jsonb)",
        "pct": "progress=CAST(:detail AS jsonb)",
    }
    await case.database.status(
        text(f"UPDATE {case.schema}.import_run SET {assignments_dict[damage]} WHERE run_id=:run"),
        run=run_id,
        marker=json.dumps({"label_worker_claim" if damage == "claimed" else "label_publication": None}),
        detail=json.dumps({"code": "example"} if damage == "error" else {damage: 1}),
    )
    before_run_dict = dict(
        await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
    )

    result_dict = await _ensure_label_route(monkeypatch, run_id)

    assert result_dict["status"] == "failed"
    assert all(call.args[0] == "GET" for call in requests.call_args_list)
    assert original_job["metadata"]["uid"] == "example-worker-uid"
    assert (
        dict(await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id))
        == before_run_dict
    )
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "legacy", "job-image", "claim-image", "missing-image"])
async def test_claim_image_survives_launcher_rollout(completion_case, monkeypatch, damage):
    case = completion_case
    run_id = "original-image-run"
    await _admit_label_run(case, monkeypatch, run_id)
    actual_job = _bind_ended_label_worker(monkeypatch, run_id)
    await label_publish.claim_label_run(case.database, {"job_id": "original-message"}, run_id)
    image_a = actual_job["spec"]["template"]["spec"]["containers"][0]["image"]
    image_b = "ghcr.io/example/worker@sha256:" + "2" * 64
    run_row_dict = dict(
        await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
    )
    claim_dict = run_row_dict["metrics"]["label_worker_claim"]
    assert claim_dict["worker_image"] == image_a
    if damage in {"legacy", "missing-image"}:
        claim_dict.pop("worker_image")
        if damage == "legacy":
            claim_dict["format"] = "label-worker-claim-v1"
    elif damage == "claim-image":
        claim_dict["worker_image"] = image_b
    elif damage == "job-image":
        actual_job["spec"]["template"]["spec"]["containers"][0]["image"] = image_b
    await case.database.status(
        text(
            f"UPDATE {case.schema}.import_run SET metrics=jsonb_set(metrics,'{{label_worker_claim}}',CAST(:claim AS jsonb)) WHERE run_id=:run"
        ),
        claim=json.dumps(claim_dict),
        run=run_id,
    )
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", image_b)
    before_run_dict = dict(
        await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
    )
    if damage:
        with pytest.raises(RuntimeError):
            await label_publish.reconcile_managed_label_run(case.database, run_id)
        assert (
            dict(
                await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
            )
            == before_run_dict
        )
        if damage == "legacy":
            monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", image_a)
    if damage in {None, "legacy"}:
        result_dict = await label_publish.reconcile_managed_label_run(case.database, run_id)
        assert result_dict["status"] == "failed" and result_dict["stage_oid"] is None
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["coordinator", "control"])
async def test_late_consumer_cannot_revive_recovered_run(completion_case, monkeypatch, entry):
    case = completion_case
    run_id = "ended-run"
    await _admit_label_run(case, monkeypatch, run_id)
    _bind_ended_label_worker(monkeypatch, "launch-owner")
    await label_publish.claim_label_run(case.database, {"job_id": "owner-message"}, run_id)
    assert (await label_publish.reconcile_managed_label_run(case.database, run_id))["status"] == "failed"
    before_run_dict = dict(
        await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id)
    )
    acquire = AsyncMock(side_effect=AssertionError("late consumer reached source I/O"))
    monkeypatch.setattr(label, "_acquire_label_source", acquire)
    with pytest.raises(RuntimeError, match="active run ownership fence"):
        if entry == "control":
            await control_lifecycle.control_single_job_start({"job_id": "late-message"}, _label_control_payload(run_id))
        else:
            await label.init_label_file({"job_id": "late-message"}, {"run_id": run_id})
    acquire.assert_not_awaited()
    assert (
        dict(await case.database.first(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run"), run=run_id))
        == before_run_dict
    )
    assert await label_publish._label_state(case.database, case.schema) == case.before


def _stage_model(schema, suffix):
    table = Table("label_" + suffix, MetaData(), Column("id", String, primary_key=True), schema=schema)
    return SimpleNamespace(__table__=table, __tablename__=table.name)


@pytest.mark.asyncio
async def test_parallel_preparation_avoids_import_run_share_locks(completion_case, monkeypatch):
    case = completion_case
    run_ids = ["parallel-a", "parallel-b"]
    for run_id in run_ids:
        await _admit_label_run(case, monkeypatch, run_id)
    monkeypatch.setattr(label, "make_class", lambda _model, suffix: _stage_model(case.schema, suffix))
    original_record = label._record_label_acquisition
    arrived, ready = [], asyncio.Event()

    async def recorded(context, schema, count):
        result = await original_record(context, schema, count)
        locks = await case.database.all(
            text("SELECT mode FROM pg_locks WHERE pid=pg_backend_pid() AND relation=to_regclass(:name)"),
            name=f"{case.schema}.import_run",
        )
        assert "ShareLock" not in {row["mode"] for row in locks}
        control_lifecycle.enqueue_status_event.assert_not_called()
        arrived.append(context["control_run_id"])
        if len(arrived) == 2:
            ready.set()
        await asyncio.wait_for(ready.wait(), 5)
        return result

    monkeypatch.setattr(label, "_record_label_acquisition", recorded)
    contexts = [
        {
            "control_run_id": run_id,
            "import_date": "l" + hashlib.sha256(run_id.encode()).hexdigest()[:32],
            "context": {"label_count": 3},
        }
        for run_id in run_ids
    ]
    workers = [asyncio.create_task(label._prepare_label_stage(context, case.schema, 1)) for context in contexts]
    try:
        assert await asyncio.wait_for(asyncio.gather(*workers), 10) == run_ids
    finally:
        for worker in workers:
            if not worker.done():
                worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
    assert control_lifecycle.enqueue_status_event.call_count == 2
    for context in contexts:
        run_row_dict = await case.database.first(
            text(f"SELECT metrics FROM {case.schema}.import_run WHERE run_id=:run"), run=context["control_run_id"]
        )
        assert run_row_dict["metrics"]["label_publication_intent"]["stage_oid"] == context["label_stage_oid"]
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["rollback", "success", "projection-error"])
async def test_stage_projection_preserves_committed_mirror(completion_case, monkeypatch, outcome):
    case = completion_case
    run_id = "projection-run"
    admitted_run_dict = await _admit_label_run(case, monkeypatch, run_id)
    suffix = "l" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
    context_dict = {"control_run_id": run_id, "import_date": suffix, "context": {"label_count": 3}}
    monkeypatch.setattr(label, "make_class", lambda _model, name: _stage_model(case.schema, name))
    delivered_events = []
    mirror_metrics_dict = dict(admitted_run_dict["metrics"])
    original = case.database.transaction

    @asynccontextmanager
    async def transaction():
        async with original() as session:
            yield session
            assert delivered_events == []
            if outcome == "rollback":
                raise RuntimeError("rollback before commit")

    def project(event):
        if outcome == "projection-error":
            raise RuntimeError("projection unavailable")
        delivered_events.append(event)
        mirror_metrics_dict.update(event.get("metrics") or {})

    monkeypatch.setattr(case.database, "transaction", transaction)
    monkeypatch.setattr(control_lifecycle, "enqueue_status_event", Mock(side_effect=project))
    if outcome == "rollback":
        with pytest.raises(RuntimeError, match="rollback before commit"):
            await label._prepare_label_stage(context_dict, case.schema, 1)
        await control_lifecycle.mark_control_run(
            run_id, status="failed", phase_detail="interrupted", progress_message="failed"
        )
        assert "label_publication_intent" not in mirror_metrics_dict
        assert delivered_events[0]["status"] == "failed"
    else:
        assert await label._prepare_label_stage(context_dict, case.schema, 1) == run_id
        if outcome == "success":
            assert mirror_metrics_dict["label_publication_intent"]["stage_oid"] == context_dict["label_stage_oid"]
    oid = await case.database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{case.schema}.label_{suffix}")
    assert oid == (None if outcome == "rollback" else context_dict["label_stage_oid"])
    assert await label_publish._label_state(case.database, case.schema) == case.before


@pytest.mark.asyncio
async def test_direct_and_managed_publishers_share_stage_authority_lock_order(completion_case, monkeypatch):
    case = completion_case
    other_suffix = "direct_attempt"
    await case.database.status(
        f'CREATE TABLE "{case.schema}".label_{other_suffix} (LIKE "{case.schema}".label INCLUDING ALL)'
    )
    await case.database.status(f"INSERT INTO \"{case.schema}\".label_{other_suffix} (id) VALUES ('direct')")
    direct_oid = await case.database.scalar(
        text("SELECT to_regclass(:name)::oid"), name=f"{case.schema}.label_{other_suffix}"
    )
    original = label_publish.read_result_publication_authority
    managed_locked, direct_waiting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    managed = None

    async def authority(database, **parameters):
        if parameters.get("lock") and asyncio.current_task() is not managed:
            direct_waiting.set()
        result = await original(database, **parameters)
        if parameters.get("lock") and asyncio.current_task() is managed and not managed_locked.is_set():
            managed_locked.set()
            await release.wait()
        return result

    monkeypatch.setattr(label_publish, "read_result_publication_authority", authority)
    managed = asyncio.create_task(label_publish.complete_label_publication(case.database, case.context_dict))
    direct = None
    try:
        await asyncio.wait_for(managed_locked.wait(), 3)
        direct = asyncio.create_task(label_publish.publish_label_table(case.database, case.schema, other_suffix))
        await asyncio.wait_for(direct_waiting.wait(), 3)
        await asyncio.sleep(0.03)
        assert not direct.done()
        release.set()
        await asyncio.wait_for(asyncio.gather(managed, direct), 3)
        state_dict = await label_publish._label_state(case.database, case.schema)
        assert state_dict["table_oid"] == direct_oid
        assert state_dict["generation"]["local_generation"] == case.before["generation"]["local_generation"] + 2
        assert (await _run_row(case))["status"] == "succeeded"
        assert (await label_publish.reconcile_managed_label_run(case.database, case.run_id))[
            "publication"
        ] == "committed"
    finally:
        release.set()
        await asyncio.gather(managed, *([direct] if direct else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_both_reconcilers_acquire_authority_before_run(completion_case, monkeypatch):
    case = completion_case
    receipt = await label_publish.complete_label_publication(case.database, case.context_dict)
    original = label_publish.read_result_publication_authority
    locked, release = asyncio.Event(), asyncio.Event()

    async def authority(database, **parameters):
        result = await original(database, **parameters)
        if parameters.get("lock"):
            locked.set()
            await release.wait()
        return result

    monkeypatch.setattr(label_publish, "read_result_publication_authority", authority)
    durable = asyncio.create_task(label_publish.reconcile_managed_label_run(case.database, case.run_id))
    fresh = None
    try:
        await asyncio.wait_for(locked.wait(), 3)
        fresh = asyncio.create_task(
            label_publish._reconcile_label_publication(
                case.database, case.context_dict, case.schema, case.suffix, receipt
            )
        )
        await asyncio.sleep(0.05)
        assert not fresh.done()
        async with case.database.acquire() as connection:
            await connection.first(
                text(f"SELECT run_id FROM {case.schema}.import_run WHERE run_id=:run FOR UPDATE NOWAIT"),
                run=case.run_id,
            )
        release.set()
        recovered, observed = await asyncio.wait_for(asyncio.gather(durable, fresh), 3)
        assert recovered["status"] == "succeeded" and observed == "succeeded"
        assert await label_publish._label_state(case.database, case.schema) == {
            "table_oid": receipt["table_oid"],
            "generation": receipt["generation"],
        }
    finally:
        release.set()
        await asyncio.gather(durable, *([fresh] if fresh else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_delayed_create_retains_native_admission_lock_through_timeout(completion_case, monkeypatch):
    case = completion_case
    run_id = "delayed-startup-run"
    await _admit_label_run(case, monkeypatch, run_id)
    original_job = _bind_ended_label_worker(monkeypatch, run_id)
    requests = _retained_worker_requests(monkeypatch, original_job, pending=True)
    started, release = threading.Event(), threading.Event()

    def delayed_request(method, path, body=None):
        if method == "POST":
            started.set()
            if not release.wait(3):
                raise RuntimeError("test did not release bounded create")
            requests(method, path, body)
            raise control_workers._KubernetesApiError(0, "create response lost")
        return requests(method, path, body)

    async def bounded_route():
        async with asyncio.timeout(0.08):
            return await _ensure_label_route(monkeypatch, run_id)

    async def read_locked_run():
        async with case.database.acquire() as connection:
            return await connection.first(
                text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run FOR UPDATE"), run=run_id
            )

    monkeypatch.setattr(control_workers, "_kubernetes_request", delayed_request)
    first = asyncio.create_task(bounded_route())
    reader = second = None
    try:
        assert await asyncio.to_thread(started.wait, 3)
        reader = asyncio.create_task(read_locked_run())
        second = asyncio.create_task(_ensure_label_route(monkeypatch, run_id))
        await asyncio.sleep(0.12)
        assert not first.done() and not reader.done() and not second.done()
        release.set()
        with pytest.raises(TimeoutError):
            await first
        row_dict, result_dict = await asyncio.wait_for(asyncio.gather(reader, second), 3)
        assert row_dict["status"] == "queued" and "label_worker_claim" not in row_dict["metrics"]
        assert result_dict["status"] == "exists"
        assert sum(call.args[0] == "POST" for call in requests.call_args_list) == 1
        assert original_job["metadata"]["uid"] == "example-worker-uid"
        assert await label_publish._label_state(case.database, case.schema) == case.before
    finally:
        release.set()
        await asyncio.gather(
            first, *([reader] if reader else []), *([second] if second else []), return_exceptions=True
        )
