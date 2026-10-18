"""Host regressions for explicit producer authority and durable handoff custody."""

import asyncio
import hashlib
import json
import re
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import control_lifecycle, ndc_product, ndc_publish, ndc_stage
from tests.process import ndc_publication_fixtures


@pytest.fixture
def handoff_attempt(monkeypatch):
    monkeypatch.setenv("HLTHPRT_NDC_PUBLICATION_MODE", "handoff")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "synthetic-node")
    attempt = ndc_stage.new_ndc_attempt("synthetic-run", "rx_data")
    attempt.table_oids = {"product": 21, "package": 22}
    attempt.input_oids = {"product": 41, "package": 42}
    attempt.incumbent_oids = {"product": 11, "package": 12}
    attempt.counts.update({key: 1 for key in attempt.counts})
    return attempt


def _audit_receipt(attempt):
    return {
        "format": "ndc-publication-v1",
        "run_id": attempt.run_id,
        "attempt_id": attempt.suffix,
        "complete": True,
        "acquisition": {"complete": True},
        "counts": dict(attempt.counts),
        "tables": {name: {"oid": oid} for name, oid in attempt.table_oids.items()},
    }


def _coordinator_stubs(monkeypatch, attempt):
    monkeypatch.setattr(ndc_product, "new_ndc_attempt", Mock(return_value=attempt))
    monkeypatch.setattr(ndc_product, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(ndc_product, "create_ndc_stages", AsyncMock())
    monkeypatch.setattr(ndc_product, "acquire_ndc_manifest", AsyncMock(return_value={}))
    monkeypatch.setattr(ndc_product, "consume_ndc_partitions", AsyncMock(return_value={"complete": True}))
    monkeypatch.setattr(ndc_product, "enqueue_status_event", Mock())
    monkeypatch.setattr(ndc_product, "enqueue_live_progress", Mock())
    monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", "https://example.test/manifest")


@pytest.mark.parametrize("mode", [None, "direct", "handoff", "", "automatic", "HANDOFF", " handoff"])
def test_publication_mode_is_strict(monkeypatch, mode):
    monkeypatch.delenv("HLTHPRT_NDC_PUBLICATION_MODE", raising=False)
    if mode is not None:
        monkeypatch.setenv("HLTHPRT_NDC_PUBLICATION_MODE", mode)
    if mode in {None, "direct", "handoff"}:
        assert ndc_stage.ndc_publication_mode() == (mode or "direct")
    else:
        with pytest.raises(ValueError, match="must be direct or handoff"):
            ndc_stage.new_ndc_attempt("synthetic-run", "rx_data")


@pytest.mark.parametrize(
    "node_id,schema,run_id",
    [
        (None, "rx_data", "synthetic-run"),
        ("", "rx_data", "synthetic-run"),
        (" synthetic-node ", "rx_data", "synthetic-run"),
        ("synthetic-node", "other_schema", "synthetic-run"),
        ("synthetic-node", "rx_data", "é" * 65),
        ("synthetic-node", "rx_data", "synthetic\nrun"),
    ],
)
def test_handoff_configuration_is_closed(monkeypatch, handoff_attempt, node_id, schema, run_id):
    if node_id is None:
        monkeypatch.delenv("HLTHPRT_IMPORT_NODE_ID")
    else:
        monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", node_id)
    with pytest.raises(ValueError, match="handoff requires"):
        ndc_stage.new_ndc_attempt(run_id, schema)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "run_id", ["synthetic run", "synthetic/run", "synthetic\\run", "synthetic-é", "_synthetic", "a" * 129]
)
async def test_handoff_run_id_refused_before_work(monkeypatch, handoff_attempt, run_id):
    new_attempt = ndc_stage.new_ndc_attempt
    _coordinator_stubs(monkeypatch, handoff_attempt)
    monkeypatch.setattr(ndc_product, "new_ndc_attempt", new_attempt)
    monkeypatch.setattr(ndc_product, "publish_ndc_tables", AsyncMock(return_value={"complete": True}))
    with pytest.raises(ValueError, match="handoff requires"):
        await ndc_product.init_file({}, {"run_id": run_id})
    for operation in (
        "ensure_import_run_table",
        "create_ndc_stages",
        "acquire_ndc_manifest",
        "consume_ndc_partitions",
        "publish_ndc_tables",
    ):
        getattr(ndc_product, operation).assert_not_awaited()


@pytest.mark.parametrize(
    "run_id", ["synthetic run", "synthetic/run", "synthetic\\run", "synthetic-é", "_synthetic", "a" * 129]
)
def test_direct_run_id_compatibility(monkeypatch, run_id):
    monkeypatch.setenv("HLTHPRT_NDC_PUBLICATION_MODE", "direct")
    assert ndc_stage.new_ndc_attempt(run_id, "rx_data").run_id == run_id


@pytest.mark.parametrize("run_id", ["0", "A0_.:@-9", "a" * 128])
def test_handoff_machine_run_id_is_exact(handoff_attempt, run_id):
    assert ndc_stage.new_ndc_attempt(run_id, "rx_data").run_id == run_id


@pytest.mark.asyncio
@pytest.mark.parametrize("job_id", [None, "synthetic-task"])
async def test_generated_handoff_run_id_is_admissible(monkeypatch, handoff_attempt, job_id):
    new_attempt = ndc_stage.new_ndc_attempt
    _coordinator_stubs(monkeypatch, handoff_attempt)
    monkeypatch.setattr(ndc_product, "new_ndc_attempt", new_attempt)
    monkeypatch.setattr(ndc_product, "publish_ndc_tables", AsyncMock(return_value={"format": "ndc-stage-handoff-v1"}))
    await ndc_product.init_file({"job_id": job_id} if job_id else {})
    claimed = ndc_product.create_ndc_stages.await_args
    assert re.fullmatch(r"ndc-[0-9a-f]{32}", claimed.args[1].run_id)
    assert claimed.args[1].publication_mode == "handoff"
    assert claimed.kwargs == {"standalone": True}


@pytest.mark.parametrize(
    "job_id,brand_name,is_valid",
    [
        ("01234567-89ab-cdef-0123-456789abcdef", "Synthetic A", True),
        ("01234567-89ab-cdef-0123-456789abcdef", "a" * 64, True),
        ("not-a-uuid", "Synthetic A", False),
        ("01234567-89AB-CDEF-0123-456789ABCDEF", "Synthetic A", False),
        ("01234567-89ab-cdef-0123-456789abcdef", "", False),
        ("01234567-89ab-cdef-0123-456789abcdef", "a" * 65, False),
        ("01234567-89ab-cdef-0123-456789abcdef", " Synthetic A", False),
        ("01234567-89ab-cdef-0123-456789abcdef", "Synthetic\nA", False),
        ("01234567-89ab-cdef-0123-456789abcdef", "Synthetic é", False),
    ],
)
def test_producer_entry_bounds(job_id, brand_name, is_valid):
    arguments = ["--job-id", job_id, "--brand-name", brand_name]
    if is_valid:
        parsed = ndc_publication_fixtures._producer_arguments(arguments)
        assert (parsed.job_id, parsed.brand_name) == (job_id, brand_name)
    else:
        with pytest.raises(SystemExit) as raised:
            ndc_publication_fixtures._producer_arguments(arguments)
        assert raised.value.code == 2


@pytest.mark.parametrize("is_namespace", [False, True])
def test_producer_entry_refuses_foreign_modules(monkeypatch, tmp_path, is_namespace):
    ndc_publication_fixtures._require_local_producer_modules()
    foreign = (
        SimpleNamespace(__path__=[str(tmp_path)])
        if is_namespace
        else SimpleNamespace(__file__=str(tmp_path / "foreign.py"))
    )
    monkeypatch.setitem(ndc_publication_fixtures.sys.modules, "process.foreign", foreign)
    with pytest.raises(AssertionError, match="foreign producer module"):
        ndc_publication_fixtures._require_local_producer_modules()


@pytest.mark.asyncio
async def test_producer_entry_requires_disposable_environment(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ENVIRONMENT", raising=False)
    with pytest.raises(AssertionError, match="disposable PostgreSQL test database"):
        await ndc_publication_fixtures._run_coordinator("01234567-89ab-cdef-0123-456789abcdef", "Synthetic A")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["direct", "handoff"])
async def test_request_cannot_select_authority(monkeypatch, mode):
    monkeypatch.setenv("HLTHPRT_NDC_PUBLICATION_MODE", mode)
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "synthetic-node")
    attempt = ndc_stage.new_ndc_attempt("synthetic-run", "rx_data")
    new_attempt = ndc_product.new_ndc_attempt
    _coordinator_stubs(monkeypatch, attempt)
    monkeypatch.setattr(ndc_product, "new_ndc_attempt", new_attempt)
    monkeypatch.setattr(ndc_product, "publish_ndc_tables", AsyncMock(return_value={"complete": True}))
    task_payload_dict = {
        "run_id": "synthetic-run",
        "publication_mode": "handoff" if mode == "direct" else "direct",
        "node_id": "untrusted-node",
        "ndc_handoff": {"published": False},
    }
    await ndc_product.init_file({}, task_payload_dict)
    claimed = ndc_product.create_ndc_stages.await_args.args[1]
    assert claimed.publication_mode == mode
    assert claimed.node_id == ("synthetic-node" if mode == "handoff" else None)


@pytest.mark.asyncio
async def test_unowned_call_cannot_bypass_handoff(handoff_attempt):
    database = SimpleNamespace(status=AsyncMock())
    with pytest.raises(ValueError, match="owned coordinator"):
        await ndc_publish.publish_ndc_tables(database, "rx_data", handoff_attempt.suffix)
    database.status.assert_not_awaited()
    with pytest.raises(RuntimeError, match="direct publication is disabled"):
        await ndc_stage.finish_ndc_publication(database, handoff_attempt, {}, published=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_standalone,can_claim", [(False, False), (False, True), (True, True)])
async def test_claim_binds_process_node(handoff_attempt, is_standalone, can_claim):
    connection = SimpleNamespace(run_sync=AsyncMock())
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))

    @asynccontextmanager
    async def transaction():
        yield session

    database = SimpleNamespace(
        transaction=transaction,
        status=AsyncMock(return_value=int(can_claim)),
        scalar=AsyncMock(side_effect=[11, 12, 21, 22, 41, 42]),
    )
    if can_claim:
        await ndc_stage.create_ndc_stages(database, handoff_attempt, standalone=is_standalone)
        assert connection.run_sync.await_count == 4
    else:
        with pytest.raises(RuntimeError, match="no longer claimable"):
            await ndc_stage.create_ndc_stages(database, handoff_attempt, standalone=is_standalone)
        connection.run_sync.assert_not_awaited()
    statements = [call for call in database.status.await_args_list if "import_run" in str(call.args[0])]
    for statement in statements:
        assert statement.kwargs["node_id"] == "synthetic-node"
    claim_sql = str(statements[-1].args[0])
    assert "engine='drug-api' AND node_id=:node_id" in claim_sql
    assert "NOT (COALESCE(metrics, '{}'::jsonb) ? 'ndc_attempt_id')" in claim_sql
    assert "status IN ('queued', 'starting', 'running')" in claim_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["save-owner", "failure", "cleanup", "sample"])
async def test_owner_changes_are_fenced(monkeypatch, handoff_attempt, operation):
    @asynccontextmanager
    async def transaction():
        yield

    database = SimpleNamespace(
        transaction=transaction, scalar=AsyncMock(return_value=None), status=AsyncMock(return_value=0)
    )
    monkeypatch.setattr(ndc_stage, "ndc_table_oids", AsyncMock(return_value=handoff_attempt.table_oids))
    if operation == "failure":
        assert await ndc_stage.fail_ndc_attempt(database, handoff_attempt) == 0
    else:
        with pytest.raises(RuntimeError):
            if operation == "save-owner":
                await ndc_stage._check_ndc_run_owner(database, handoff_attempt)
            elif operation == "cleanup":
                await ndc_stage.discard_ndc_stages(database, handoff_attempt)
            else:
                await ndc_stage.finish_ndc_publication(database, handoff_attempt, {"complete": False}, published=False)
    guarded_call = database.status.await_args if operation in {"failure", "sample"} else database.scalar.await_args
    assert guarded_call.kwargs["node_id"] == "synthetic-node"
    assert "engine='drug-api' AND node_id=:node_id" in str(guarded_call.args[0])
    assert all("DROP" not in str(call.args[0]) for call in database.status.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "sample", "stage", "busy", "incumbent", "cas", "census", "size", "mode"])
async def test_handoff_guards_and_custody(monkeypatch, handoff_attempt, damage):
    attempt = handoff_attempt
    receipt_dict = _audit_receipt(attempt)
    if damage == "sample":
        receipt_dict["complete"] = False
    if damage == "census":
        attempt.counts["package"] = 0
    if damage == "size":
        receipt_dict["acquisition"]["unbounded"] = "x" * (128 * 1024)
    if damage == "mode":
        attempt.publication_mode = "direct"
    catalog_pairs = [
        {} if damage == "stage" else attempt.table_oids,
        {} if damage == "incumbent" else attempt.incumbent_oids,
    ]
    monkeypatch.setattr(ndc_stage, "ndc_table_oids", AsyncMock(side_effect=catalog_pairs))
    database = SimpleNamespace(
        status=AsyncMock(return_value=int(damage != "cas")), scalar=AsyncMock(side_effect=[damage != "busy", 31, 32])
    )
    if damage:
        with pytest.raises(RuntimeError):
            await ndc_stage.handoff_ndc_stages(database, attempt, receipt_dict)
        writes = [call for call in database.status.await_args_list if not str(call.args[0]).startswith("LOCK TABLE")]
        assert len(writes) == int(damage == "cas")
        return
    handoff_dict = await ndc_stage.handoff_ndc_stages(database, attempt, receipt_dict)
    _assert_handoff_receipt(attempt, handoff_dict)
    *locks, update, comment_product, comment_package, drop = database.status.await_args_list
    assert len(locks) == 2 and all("IN ACCESS SHARE MODE NOWAIT" in call.args[0] for call in locks)
    assert database.scalar.await_args_list[0].kwargs["scope"] == "rx_data:ndc-publication"
    comments = [comment_product, comment_package]
    assert update.kwargs["node_id"] == attempt.node_id
    transition = str(update.args[0])
    assert "status='finalizing'" in transition and "status='running'" in transition
    assert "finished_at=NULL, error=NULL" in transition and "node_id=:node_id" in transition
    assert "ARRAY['ndc_handoff','ndc_publication','ndc_sample']" in transition
    assert "params->>'test_mode'" in transition and "params->>'test'" in transition
    marker_dict = {key: handoff_dict[key] for key in ("format", "run_id", "attempt_id", "handoff_sha256")}
    assert len(comments) == 2
    for comment in comments:
        assert json.dumps(marker_dict, sort_keys=True) in comment.args[0]
    assert drop.args[0] == "DROP TABLE " + ", ".join(
        f"{attempt.schema}.{table.name}" for table in attempt.input_tables.values()
    )
    assert all("ALTER TABLE" not in str(call.args[0]) for call in database.status.await_args_list)


def _assert_handoff_receipt(attempt, handoff_dict):
    assert handoff_dict["format"] == "ndc-stage-handoff-v1"
    assert handoff_dict["complete"] is True and handoff_dict["published"] is False
    assert handoff_dict["database_oid"] == 31 and handoff_dict["import_run_oid"] == 32
    assert handoff_dict["incumbent_oids"] == attempt.incumbent_oids
    assert "completed_at" not in handoff_dict
    assert handoff_dict["tables"] == {
        name: {"oid": oid, "name": attempt.tables[name].name} for name, oid in attempt.table_oids.items()
    }
    digest_payload_dict = {
        key: entry for key, entry in handoff_dict.items() if key not in {"handed_off_at", "handoff_sha256"}
    }
    encoded = json.dumps(
        digest_payload_dict, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()
    assert handoff_dict["handoff_sha256"] == hashlib.sha256(encoded).hexdigest()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_complete", [False, True])
async def test_handoff_never_swaps_live_tables(monkeypatch, handoff_attempt, is_complete):
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        yield "session"
        events.append("commit")

    def recording(name, receipt=None):
        async def operation(*_args, **_kwargs):
            events.append(name)
            return receipt

        return operation

    for name, event in (
        ("lock_ndc_stages", "lock"),
        ("complete_ndc_stages", "complete"),
        ("audit_ndc_tables", "audit"),
        ("handoff_ndc_stages", "handoff"),
        ("finish_ndc_publication", "sample"),
    ):
        monkeypatch.setattr(ndc_publish, name, recording(event, {"complete": is_complete}))
    monkeypatch.setattr(ndc_publish, "_publish_single_ndc_table", AsyncMock(side_effect=AssertionError("live swap")))
    database = SimpleNamespace(transaction=transaction, status=AsyncMock())
    await ndc_publish.publish_ndc_tables(
        database,
        handoff_attempt.schema,
        handoff_attempt.suffix,
        attempt=handoff_attempt,
        acquisition={"complete": is_complete},
    )
    assert events == ["begin", "lock", "complete", "audit", "handoff" if is_complete else "sample", "commit"]
    assert all(str(call.args[0]).startswith("CREATE EXTENSION") for call in database.status.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["complete", "handoff", "commit", "cancel"])
async def test_failure_restores_completion_counts(monkeypatch, handoff_attempt, failure):
    before_dict = dict(handoff_attempt.counts)

    @asynccontextmanager
    async def transaction():
        yield "session"
        if failure == "commit":
            raise RuntimeError("commit")
        if failure == "cancel":
            raise asyncio.CancelledError()

    async def complete(*_args):
        handoff_attempt.counts.update(product=2, package=3)
        if failure == "complete":
            raise RuntimeError("complete")

    monkeypatch.setattr(ndc_publish, "lock_ndc_stages", AsyncMock())
    monkeypatch.setattr(ndc_publish, "complete_ndc_stages", complete)
    monkeypatch.setattr(ndc_publish, "audit_ndc_tables", AsyncMock(return_value={}))
    monkeypatch.setattr(
        ndc_publish,
        "handoff_ndc_stages",
        AsyncMock(side_effect=RuntimeError("handoff") if failure == "handoff" else None),
    )
    database = SimpleNamespace(transaction=transaction, status=AsyncMock())
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
        await ndc_publish.publish_ndc_tables(
            database,
            handoff_attempt.schema,
            handoff_attempt.suffix,
            attempt=handoff_attempt,
            acquisition={"complete": True},
        )
    assert handoff_attempt.counts == before_dict


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "COMMENT ON TABLE", "DROP TABLE"])
async def test_handoff_writes_share_one_transaction(monkeypatch, handoff_attempt, failure):
    committed_statements = []
    pending_statements = []
    active_transactions = []

    @asynccontextmanager
    async def transaction():
        active_transactions.append(True)
        try:
            yield "session"
            committed_statements.extend(pending_statements)
        finally:
            pending_statements.clear()
            active_transactions.pop()

    async def status(statement, **_options):
        sql = str(statement)
        if sql.startswith("CREATE EXTENSION"):
            assert not active_transactions
            return 0
        assert active_transactions == [True]
        pending_statements.append(sql)
        if failure and sql.startswith(failure):
            raise RuntimeError("atomic write failed")
        return 1

    monkeypatch.setattr(ndc_publish, "lock_ndc_stages", AsyncMock())
    monkeypatch.setattr(ndc_publish, "complete_ndc_stages", AsyncMock())
    monkeypatch.setattr(ndc_publish, "audit_ndc_tables", AsyncMock(return_value=_audit_receipt(handoff_attempt)))
    monkeypatch.setattr(
        ndc_stage, "ndc_table_oids", AsyncMock(side_effect=[handoff_attempt.table_oids, handoff_attempt.incumbent_oids])
    )
    database = SimpleNamespace(transaction=transaction, status=status, scalar=AsyncMock(side_effect=[True, 31, 32]))
    publish = ndc_publish.publish_ndc_tables(
        database,
        handoff_attempt.schema,
        handoff_attempt.suffix,
        attempt=handoff_attempt,
        acquisition={"complete": True},
    )
    if failure:
        with pytest.raises(RuntimeError, match="atomic write failed"):
            await publish
        assert committed_statements == []
    else:
        receipt_dict = await publish
        assert receipt_dict["published"] is False
        assert len(committed_statements) == 6
        assert all("IN ACCESS SHARE MODE NOWAIT" in sql for sql in committed_statements[:2])
        assert "status='finalizing'" in committed_statements[2]
        assert all(sql.startswith("COMMENT ON TABLE") for sql in committed_statements[3:5])
        assert committed_statements[-1].startswith("DROP TABLE")


@pytest.mark.asyncio
@pytest.mark.parametrize("is_canceled", [False, True])
async def test_commit_ambiguity_retains_handoff(monkeypatch, handoff_attempt, is_canceled):
    attempt = handoff_attempt
    _coordinator_stubs(monkeypatch, attempt)
    monkeypatch.setattr(
        ndc_stage,
        "ndc_table_oids",
        AsyncMock(side_effect=[attempt.table_oids, attempt.incumbent_oids, attempt.table_oids]),
    )

    @asynccontextmanager
    async def transaction():
        yield

    async def status(statement, **_options):
        if "status='failed'" in str(statement):
            assert "status='running'" in str(statement)
            return 0
        return 1

    database = SimpleNamespace(
        transaction=transaction,
        status=AsyncMock(side_effect=status),
        scalar=AsyncMock(side_effect=[True, 31, 32, None]),
    )
    monkeypatch.setattr(ndc_product, "db", database)

    async def commit_then_interrupt(*_args, **_kwargs):
        await ndc_stage.handoff_ndc_stages(database, attempt, _audit_receipt(attempt))
        raise asyncio.CancelledError() if is_canceled else RuntimeError("commit acknowledgement lost")

    monkeypatch.setattr(ndc_product, "publish_ndc_tables", commit_then_interrupt)
    with pytest.raises(asyncio.CancelledError if is_canceled else RuntimeError):
        await ndc_product.init_file({}, {"run_id": attempt.run_id})
    ndc_product.enqueue_status_event.assert_not_called()
    dropped_statements = [
        call.args[0] for call in database.status.await_args_list if str(call.args[0]).startswith("DROP")
    ]
    assert len(dropped_statements) == 1 and all(
        table.name in dropped_statements[0] for table in attempt.input_tables.values()
    )
    assert all(table.name not in dropped_statements[0] for table in attempt.tables.values())
    cleanup_guard = str(database.scalar.await_args.args[0])
    assert "NOT (COALESCE(metrics, '{}'::jsonb) ? 'ndc_handoff')" in cleanup_guard
    assert "FOR SHARE" in cleanup_guard


@pytest.mark.parametrize("event_error", [False, True])
def test_handoff_event_is_not_terminal(monkeypatch, handoff_attempt, event_error):
    status_event = Mock()
    live_progress = Mock(side_effect=RuntimeError("event unavailable") if event_error else None)
    monkeypatch.setattr(ndc_product, "enqueue_status_event", status_event)
    monkeypatch.setattr(ndc_product, "enqueue_live_progress", live_progress)
    ndc_product._announce_ndc_completion(handoff_attempt, {"format": "ndc-stage-handoff-v1"})
    status_event.assert_not_called()
    assert live_progress.call_args.kwargs["status"] == "finalizing"
    assert "pct" not in live_progress.call_args.kwargs


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "published", "incomplete", "run", "module"])
async def test_control_handoff_result_is_nonterminal(monkeypatch, damage):
    control_task = control_lifecycle.ControlTask(
        "synthetic-run", "ndc", "process.ndc_product", "init_file", "ctx_task", {}
    )
    receipt_dict = {"format": "ndc-stage-handoff-v1", "run_id": "synthetic-run", "complete": True, "published": False}
    if damage == "published":
        receipt_dict["published"] = True
    elif damage == "incomplete":
        receipt_dict["complete"] = False
    elif damage == "run":
        receipt_dict["run_id"] = "other-run"
    elif damage == "module":
        control_task = replace(control_task, target_module="process.other")
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    wrapper = await control_lifecycle._finish_native_result(control_task, receipt_dict)
    assert wrapper == {
        "status": "running" if damage else "finalizing",
        "run_id": "synthetic-run",
        "result": receipt_dict,
    }
    control_lifecycle._flush_terminal_status_events.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_index_names_are_preserved(monkeypatch):
    monkeypatch.setenv("HLTHPRT_NDC_PUBLICATION_MODE", "direct")
    attempt = ndc_stage.new_ndc_attempt("synthetic-run", "rx_data")
    database = SimpleNamespace(status=AsyncMock())
    await ndc_publish._rename_model_stage_indexes(database, attempt)
    expected_statements = []
    for model in (ndc_stage.Product, ndc_stage.Package):
        table = attempt.tables[model.__tablename__]
        for index in model.__my_additional_indexes__:
            declaration = str(
                ndc_stage.declared_index_statements(
                    model, table, attempt.schema, (index,), index_schema_name=table.name
                )[0]
            )
            actual_name = declaration.split('"')[1]
            expected_statements.append(
                f'ALTER INDEX "rx_data"."{actual_name}" RENAME TO "{index["staging_name"]}_{attempt.suffix}"'
            )
    assert [call.args[0] for call in database.status.await_args_list] == expected_statements


@pytest.mark.asyncio
async def test_handoff_database_registers_exact_cleanup():
    from tests.process import test_ndc_publication_proof_postgres as native_proof

    events = []
    connection = SimpleNamespace(
        scalar=AsyncMock(side_effect=[None, 41, "synthetic_ci", 41, None]),
        execute=AsyncMock(side_effect=lambda statement: events.append(str(statement))),
    )
    async with AsyncExitStack() as cleanup:

        def register(callback, *args):
            events.append("registered")
            cleanup.push_async_callback(callback, *args)

        allocation = await native_proof._allocate_handoff_database(
            connection, SimpleNamespace(push_async_callback=register)
        )
        assert allocation.oid == 41
        assert re.fullmatch(r"ndc_handoff_test_[0-9a-f]{32}", allocation.name)
    assert events == [
        "registered",
        f'CREATE DATABASE "{allocation.name}" TEMPLATE template0',
        f'DROP DATABASE "{allocation.name}"',
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["name", "caller", "unknown", "changed", "boolean"])
async def test_handoff_database_refuses_unsafe_cleanup(damage):
    from tests.process import test_ndc_publication_proof_postgres as native_proof

    allocation = SimpleNamespace(name="ndc_handoff_test_" + "1" * 32, oid=41)
    if damage == "name":
        allocation.name = "synthetic_ci"
    elif damage in {"unknown", "boolean"}:
        allocation.oid = None if damage == "unknown" else True
    connection = SimpleNamespace(
        scalar=AsyncMock(
            side_effect=[allocation.name if damage == "caller" else "synthetic_ci", 42 if damage == "changed" else 41]
        ),
        execute=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="database identity|caller database|ownership is unproved or changed"):
        await native_proof._drop_handoff_database(connection, allocation)
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_handoff_database_refuses_existing_name():
    from tests.process import test_ndc_publication_proof_postgres as native_proof

    connection = SimpleNamespace(scalar=AsyncMock(return_value=41), execute=AsyncMock())
    cleanup = SimpleNamespace(push_async_callback=Mock())
    with pytest.raises(AssertionError):
        await native_proof._allocate_handoff_database(connection, cleanup)
    connection.execute.assert_not_awaited()
    cleanup.push_async_callback.assert_not_called()


@pytest.mark.asyncio
async def test_handoff_database_retains_uncertain_create():
    from tests.process import test_ndc_publication_proof_postgres as native_proof

    connection = SimpleNamespace(
        scalar=AsyncMock(side_effect=[None, "synthetic_ci", 41]),
        execute=AsyncMock(side_effect=RuntimeError("synthetic creation acknowledgement failure")),
    )
    with pytest.raises(RuntimeError, match="ownership is unproved or changed"):
        async with AsyncExitStack() as cleanup:
            await native_proof._allocate_handoff_database(connection, cleanup)
    connection.execute.assert_awaited_once()
    assert str(connection.execute.await_args.args[0]).startswith("CREATE DATABASE ")
