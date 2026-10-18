"""Native SQL ownership and finalization seams exercised without a database."""

import hashlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import ndc_publish, ndc_stage


def _attempt():
    attempt = ndc_stage.new_ndc_attempt("synthetic-run", "rx_data")
    attempt.table_oids = {"product": 21, "package": 22}
    attempt.incumbent_oids = {"product": 11, "package": 12}
    attempt.counts.update(product=1, package=1, source_products=1)
    return attempt


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", ["valid", "oid", "comment", "owner"])
async def test_staging_checks_exact_physical_owner_and_active_run(monkeypatch, guard):
    attempt = _attempt()
    oids_by_name = dict(attempt.table_oids)
    if guard == "oid":
        oids_by_name["product"] = 99
    monkeypatch.setattr(ndc_stage, "ndc_table_oids", AsyncMock(return_value=oids_by_name))
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=attempt.owner_comment))
    if guard == "comment":
        database.scalar.return_value = "foreign"
    if guard in {"oid", "comment"}:
        with pytest.raises(RuntimeError, match="changed"):
            await ndc_stage.lock_ndc_stages(database, attempt)
    else:
        await ndc_stage.lock_ndc_stages(database, attempt, exclusive=True)
        database.scalar.return_value = None if guard == "owner" else attempt.run_id
        if guard == "owner":
            with pytest.raises(RuntimeError, match="ownership"):
                await ndc_stage._check_ndc_run_owner(database, attempt)
        else:
            await ndc_stage._check_ndc_run_owner(database, attempt)
    statements = [str(call.args[0]) for call in database.status.call_args_list]
    assert statements[:2] == ["SET LOCAL lock_timeout = '5s'", "SET LOCAL statement_timeout = '120s'"]
    assert statements[2].startswith("LOCK TABLE rx_data.package_input_")
    assert all(table.name in statements[2] for table in (*attempt.tables.values(), *attempt.input_tables.values()))


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", ["valid", "busy", "incumbent"])
@pytest.mark.parametrize("mode", ["direct", "handoff"])
async def test_live_publication_uses_nonwaiting_lock_and_exact_predecessor(monkeypatch, guard, mode):
    attempt = _attempt()
    attempt.publication_mode = mode
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=guard != "busy"))
    monkeypatch.setattr(
        ndc_stage, "ndc_table_oids", AsyncMock(return_value={} if guard == "incumbent" else attempt.incumbent_oids)
    )
    if guard == "valid":
        await ndc_stage.check_ndc_incumbents(database, attempt)
    else:
        with pytest.raises(RuntimeError):
            await ndc_stage.check_ndc_incumbents(database, attempt)
    if guard == "busy":
        database.status.assert_not_called()
    else:
        assert database.status.await_count == 2
        expected_mode = "ACCESS SHARE" if mode == "handoff" else "ACCESS EXCLUSIVE"
        assert all(f"IN {expected_mode} MODE NOWAIT" in call.args[0] for call in database.status.call_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "source_census", "stage_census", "copy_census"])
async def test_audit_binds_full_native_csv_hash_rows_and_schema(failure):
    attempt = _attempt()
    csv_bytes = b'"quoted\nrow",\xc3\xa9\n'

    async def copy_from_query(_query, **options):
        assert options["format"] == "csv" and options["timeout"] == 120
        await options["output"](bytearray(csv_bytes[:5]))
        await options["output"](bytearray(csv_bytes[5:]))
        return "COPY 2" if failure == "copy_census" else "COPY 1"

    async def fetchval(query, relation=None):
        if "to_regclass" in query:
            return 21 if '"product_' in relation else 22
        return 2 if failure == "stage_census" else 1

    driver = SimpleNamespace(
        copy_from_query=copy_from_query,
        fetchval=fetchval,
        fetch=AsyncMock(return_value=[{"name": "key", "type": "text", "not_null": True}]),
    )
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    database = SimpleNamespace(
        status=AsyncMock(),
        scalar=AsyncMock(return_value=2 if failure == "stage_census" else 1),
        all=AsyncMock(return_value=[{"name": "key", "type": "text", "not_null": True}]),
    )
    acquisition_dict = {"partitions": [{"records": 2 if failure == "source_census" else 1}], "complete": True}
    if failure:
        with pytest.raises(RuntimeError, match="census"):
            await ndc_stage.audit_ndc_tables(database, session, attempt, acquisition_dict)
    else:
        receipt = await ndc_stage.audit_ndc_tables(database, session, attempt, acquisition_dict)
        assert receipt["counts"] == attempt.counts
        for name, summary in receipt["tables"].items():
            assert summary["oid"] == attempt.table_oids[name]
            assert summary["sha256"] == hashlib.sha256(csv_bytes).hexdigest()
            assert summary["size_bytes"] == len(csv_bytes)
            assert summary["row_count"] == 1
            assert summary["columns"] == [{"name": "key", "type": "text", "not_null": True}]
    if failure == "source_census":
        database.status.assert_not_awaited()
    else:
        database.status.assert_awaited_once_with("SET LOCAL DateStyle TO 'ISO, YMD'")


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", ["valid", "oid", "terminal"])
@pytest.mark.parametrize("is_published", [False, True])
async def test_final_receipt_comments_require_exact_pair_and_successful_run_cas(monkeypatch, guard, is_published):
    attempt = _attempt()
    database = SimpleNamespace(status=AsyncMock(return_value=0 if guard == "terminal" else 1))
    monkeypatch.setattr(
        ndc_stage, "ndc_table_oids", AsyncMock(return_value={} if guard == "oid" else attempt.table_oids)
    )
    monkeypatch.setattr(ndc_stage, "discard_ndc_stages", AsyncMock())
    receipt_dict = {"format": "ndc-publication-v1", "complete": is_published}
    if guard != "valid":
        with pytest.raises(RuntimeError):
            await ndc_stage.finish_ndc_publication(database, attempt, receipt_dict, published=is_published)
        assert database.status.call_count == (1 if guard == "terminal" else 0)
        return
    result = await ndc_stage.finish_ndc_publication(database, attempt, receipt_dict, published=is_published)
    assert result["published"] is is_published
    assert result["completed_at"]
    transition = database.status.call_args_list[0]
    assert "AND metrics->>'ndc_attempt_id'=:attempt_id AND status='running'" in str(transition.args[0])
    assert json.loads(transition.kwargs["metrics"])["ndc_publication" if is_published else "ndc_sample"] == result
    assert ndc_stage.discard_ndc_stages.await_count == int(not is_published)
    assert len(database.status.call_args_list) == (4 if is_published else 1)
    for call in database.status.call_args_list[1:3]:
        assert call.args[0].startswith("COMMENT ON TABLE rx_data.")
        assert json.dumps(result, sort_keys=True) in call.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("is_complete", [False, True])
async def test_expensive_owned_stage_work_precedes_live_locks(monkeypatch, is_complete):
    attempt = _attempt()
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        yield "session"
        events.append("commit")

    async def status(statement):
        if statement.startswith("CREATE INDEX"):
            events.append("index")

    async def stages(*_args, **_kwargs):
        events.append("stage-lock")

    async def audit(*_args):
        events.append("audit")
        return {"complete": is_complete}

    async def complete(*_args):
        events.append("complete-models")

    async def live(*_args):
        events.append("live-lock")

    async def swap(*_args):
        events.append("swap")

    async def finish(*_args, **_kwargs):
        events.append("terminal")
        return {"complete": is_complete}

    database = SimpleNamespace(transaction=transaction, status=status)
    for name, function in [
        ("lock_ndc_stages", stages),
        ("audit_ndc_tables", audit),
        ("complete_ndc_stages", complete),
        ("_rename_model_stage_indexes", AsyncMock()),
        ("check_ndc_incumbents", live),
        ("_publish_single_ndc_table", swap),
        ("finish_ndc_publication", finish),
    ]:
        monkeypatch.setattr(ndc_publish, name, function)
    await ndc_publish.publish_ndc_tables(
        database, attempt.schema, attempt.suffix, attempt=attempt, acquisition={"complete": is_complete}
    )
    assert events[0:2] == ["begin", "stage-lock"]
    assert events[-2:] == ["terminal", "commit"]
    if is_complete:
        assert events.index("complete-models") < events.index("audit")
        assert events.index("audit") < events.index("live-lock") < events.index("swap") < events.index("terminal")
    else:
        assert events == ["begin", "stage-lock", "complete-models", "audit", "terminal", "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("is_standalone,can_claim", [(False, False), (False, True), (True, True)])
async def test_creation_claims_once_and_registers_exact_stages_without_drops(is_standalone, can_claim):
    attempt = _attempt()
    created_names = []
    events = []

    async def run_sync(create_table):
        created_names.append(create_table.__self__.name)

    connection = SimpleNamespace(run_sync=run_sync)
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))

    @asynccontextmanager
    async def transaction():
        try:
            yield session
        except Exception:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    database = SimpleNamespace(
        transaction=transaction,
        status=AsyncMock(return_value=int(can_claim)),
        scalar=AsyncMock(side_effect=[11, 12, 21, 22, 31, 32]),
    )
    if not can_claim:
        with pytest.raises(RuntimeError, match="claimable"):
            await ndc_stage.create_ndc_stages(database, attempt, standalone=is_standalone)
        assert not created_names
        assert events == ["rollback"]
        return
    await ndc_stage.create_ndc_stages(database, attempt, standalone=is_standalone)
    assert created_names == [
        f"product_{attempt.suffix}",
        f"package_{attempt.suffix}",
        f"product_input_{attempt.suffix}",
        f"package_input_{attempt.suffix}",
    ]
    assert attempt.incumbent_oids == {"product": 11, "package": 12}
    assert attempt.table_oids == {"product": 21, "package": 22}
    assert attempt.input_oids == {"product": 31, "package": 32}
    assert events == ["commit"]
    assert not any("DROP " in str(call.args[0]) for call in database.status.call_args_list)
    assert sum("INSERT INTO" in str(call.args[0]) for call in database.status.call_args_list) == is_standalone


@pytest.mark.asyncio
async def test_failure_update_cannot_relabel_committed_success():
    attempt = _attempt()
    database = SimpleNamespace(status=AsyncMock(return_value=0))
    assert await ndc_stage.fail_ndc_attempt(database, attempt) == 0
    statement = str(database.status.call_args.args[0])
    assert "AND metrics->>'ndc_attempt_id'=:attempt_id AND status='running'" in statement
    assert database.status.call_args.kwargs == {"run_id": attempt.run_id, "attempt_id": attempt.suffix, "node_id": None}


def test_attempt_names_are_private_unique_and_schema_is_not_executable():
    first = _attempt()
    second = _attempt()
    assert first.suffix != second.suffix
    assert len(first.suffix) == 33
    assert first.tables["product"].name != second.tables["product"].name
    with pytest.raises(ValueError, match="schema"):
        ndc_stage.new_ndc_attempt("synthetic-run", "public; DROP SCHEMA public")


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["owned", "absent", "uncreated", "foreign"])
async def test_discard_removes_only_verified_unpublished_stages(monkeypatch, state):
    attempt = _attempt()
    if state == "uncreated":
        attempt.table_oids = {}

    @asynccontextmanager
    async def transaction():
        yield

    database = SimpleNamespace(
        transaction=transaction, status=AsyncMock(), scalar=AsyncMock(return_value=attempt.run_id)
    )
    monkeypatch.setattr(
        ndc_stage,
        "ndc_table_oids",
        AsyncMock(return_value=({"product": None, "package": None} if state == "absent" else attempt.table_oids)),
    )
    monkeypatch.setattr(
        ndc_stage,
        "lock_ndc_stages",
        AsyncMock(side_effect=RuntimeError("ownership changed") if state == "foreign" else None),
    )
    if state == "foreign":
        with pytest.raises(RuntimeError, match="ownership changed"):
            await ndc_stage.discard_ndc_stages(database, attempt)
    else:
        await ndc_stage.discard_ndc_stages(database, attempt)
    if state == "owned":
        ndc_stage.lock_ndc_stages.assert_awaited_once_with(database, attempt, exclusive=True)
        database.status.assert_awaited_once_with(
            f"DROP TABLE rx_data.product_{attempt.suffix}, rx_data.package_{attempt.suffix}, "
            f"rx_data.product_input_{attempt.suffix}, rx_data.package_input_{attempt.suffix}"
        )
    else:
        database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("allows_null", [False, True])
async def test_model_completion_order(monkeypatch, allows_null):
    attempt = _attempt()
    events = []
    relationship = ndc_stage.RELATIONSHIPS[0]
    monkeypatch.setattr(ndc_stage, "RELATIONSHIPS", (relationship[:-1] + (allows_null,),))

    async def copy(_session, _query, *, table_name, **_kwargs):
        events.append(("copy", table_name))

    async def constraints(_session, table):
        events.append(("constraints", table.name))

    async def execute(statement):
        events.append(("index", statement))

    async def scalar(statement, **_options):
        query = str(statement)
        if "GROUP BY" in query:
            return False
        if "count(*)" in query:
            return 1
        events.append(("relationship", query))
        return False

    monkeypatch.setattr(ndc_stage, "copy_projection", copy)
    monkeypatch.setattr(ndc_stage, "create_table_constraints", constraints)
    monkeypatch.setattr(ndc_stage, "declared_index_statements", lambda model, *_args, **_kwargs: (model.__tablename__,))
    database = SimpleNamespace(status=AsyncMock(), scalar=scalar)
    await ndc_stage.complete_ndc_stages(database, SimpleNamespace(execute=execute), attempt)

    assert events[:4] == [
        ("copy", attempt.tables["product"].name),
        ("copy", attempt.tables["package"].name),
        ("constraints", attempt.tables["product"].name),
        ("index", "product"),
    ]
    assert events[-1][0] == "relationship"
    assert ("IS NOT NULL AND" if allows_null else "IS NULL OR") in events[-1][1]
    assert attempt.counts["product"] == attempt.counts["package"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["conflict", "parent"])
async def test_completion_rejects_invalid_sets(monkeypatch, failure):
    attempt = _attempt()
    counts_before_dict = dict(attempt.counts)

    async def scalar(statement, **_options):
        query = str(statement)
        if "GROUP BY" in query:
            return failure == "conflict"
        if "count(*)" in query:
            return 2
        return True

    monkeypatch.setattr(ndc_stage, "copy_projection", AsyncMock())
    monkeypatch.setattr(ndc_stage, "create_table_constraints", AsyncMock())
    monkeypatch.setattr(ndc_stage, "declared_index_statements", lambda *_args, **_kwargs: ())
    database = SimpleNamespace(status=AsyncMock(), scalar=scalar)
    with pytest.raises(RuntimeError, match="missing identity|absent product"):
        await ndc_stage.complete_ndc_stages(database, SimpleNamespace(execute=AsyncMock()), attempt)
    assert attempt.counts == counts_before_dict
    assert ndc_stage.copy_projection.await_count == (0 if failure == "conflict" else 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_table", ["product", "package"])
async def test_keys_checked_before_projection(monkeypatch, invalid_table):
    attempt = _attempt()

    async def has_invalid_input(statement, **options):
        query = str(statement)
        assert 'HAVING "' in query and "IS NULL OR btrim(" in query
        assert "count(DISTINCT to_jsonb(acquired))>1" in query
        blank_chars = options["blank_key_chars"]
        assert all(character.isspace() for character in blank_chars)
        assert all(identity.strip(blank_chars) == "" for identity in ("", " \t\r\n", "\u2003", "\u3000"))
        return attempt.input_tables[invalid_table].name in query

    monkeypatch.setattr(ndc_stage, "copy_projection", AsyncMock())
    database = SimpleNamespace(status=AsyncMock(), scalar=has_invalid_input)
    with pytest.raises(RuntimeError, match="missing identity|conflicting records"):
        await ndc_stage.complete_ndc_stages(database, SimpleNamespace(execute=AsyncMock()), attempt)
    ndc_stage.copy_projection.assert_not_awaited()
    for name, input_table in attempt.input_tables.items():
        assert all(column.nullable for column in input_table.primary_key.columns)
        assert all(not column.nullable for column in attempt.tables[name].primary_key.columns)


@pytest.mark.asyncio
async def test_acquisition_oid_is_fenced(monkeypatch):
    attempt = _attempt()
    attempt.input_oids = {"product": 31, "package": 32}
    monkeypatch.setattr(ndc_stage, "ndc_table_oids", AsyncMock(return_value=attempt.table_oids))
    monkeypatch.setattr(ndc_stage, "ndc_input_oids", AsyncMock(return_value={"product": 99, "package": 32}))
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(RuntimeError, match="acquisition identity changed"):
        await ndc_stage.lock_ndc_stages(database, attempt)
    database.scalar.assert_not_awaited()
