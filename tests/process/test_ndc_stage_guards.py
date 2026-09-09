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
    assert statements[2].startswith("LOCK TABLE rx_data.product_")


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", ["valid", "busy", "incumbent"])
async def test_live_publication_uses_nonwaiting_lock_and_exact_predecessor(monkeypatch, guard):
    attempt = _attempt()
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=guard != "busy"))
    monkeypatch.setattr(ndc_stage, "ndc_table_oids", AsyncMock(return_value={} if guard == "incumbent" else attempt.incumbent_oids))
    if guard == "valid":
        await ndc_stage.check_ndc_incumbents(database, attempt)
    else:
        with pytest.raises(RuntimeError):
            await ndc_stage.check_ndc_incumbents(database, attempt)
    assert all("NOWAIT" in call.args[0] for call in database.status.call_args_list)
    if guard == "busy":
        database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "source_census", "stage_census", "copy_census"])
async def test_audit_binds_full_native_csv_hash_rows_and_schema(failure):
    attempt = _attempt()
    payload = b'"quoted\nrow",\xc3\xa9\n'

    async def copy_from_query(_query, **options):
        assert options["format"] == "csv" and options["timeout"] == 120
        await options["output"](bytearray(payload[:5]))
        await options["output"](bytearray(payload[5:]))
        return "COPY 2" if failure == "copy_census" else "COPY 1"

    driver = SimpleNamespace(copy_from_query=copy_from_query)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    database = SimpleNamespace(scalar=AsyncMock(return_value=2 if failure == "stage_census" else 1),
                               all=AsyncMock(return_value=[{"name": "key", "type": "text", "not_null": True}]))
    acquisition_dict = {"partitions": [{"records": 2 if failure == "source_census" else 1}], "complete": True}
    if failure:
        with pytest.raises(RuntimeError, match="census"):
            await ndc_stage.audit_ndc_tables(database, session, attempt, acquisition_dict)
    else:
        receipt = await ndc_stage.audit_ndc_tables(database, session, attempt, acquisition_dict)
        assert receipt["counts"] == attempt.counts
        for name, summary in receipt["tables"].items():
            assert summary["oid"] == attempt.table_oids[name]
            assert summary["sha256"] == hashlib.sha256(payload).hexdigest()
            assert summary["size_bytes"] == len(payload)
            assert summary["row_count"] == 1
            assert summary["columns"] == [{"name": "key", "type": "text", "not_null": True}]


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", ["valid", "oid", "terminal"])
@pytest.mark.parametrize("is_published", [False, True])
async def test_final_receipt_comments_require_exact_pair_and_successful_run_cas(monkeypatch, guard, is_published):
    attempt = _attempt()
    database = SimpleNamespace(status=AsyncMock(return_value=0 if guard == "terminal" else 1))
    monkeypatch.setattr(ndc_stage, "ndc_table_oids", AsyncMock(return_value={} if guard == "oid" else attempt.table_oids))
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
    assert len(database.status.call_args_list) == 3
    for call in database.status.call_args_list[1:]:
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

    async def live(*_args):
        events.append("live-lock")

    async def swap(*_args):
        events.append("swap")

    async def finish(*_args, **_kwargs):
        events.append("terminal")
        return {"complete": is_complete}

    database = SimpleNamespace(transaction=transaction, status=status)
    for name, function in [("lock_ndc_stages", stages), ("audit_ndc_tables", audit),
                           ("check_ndc_incumbents", live), ("_publish_single_ndc_table", swap),
                           ("finish_ndc_publication", finish)]:
        monkeypatch.setattr(ndc_publish, name, function)
    await ndc_publish.publish_ndc_tables(database, attempt.schema, attempt.suffix,
                                         attempt=attempt, acquisition={"complete": is_complete})
    assert events[0:2] == ["begin", "stage-lock"]
    assert events[-2:] == ["terminal", "commit"]
    if is_complete:
        assert events.count("index") == 5
        assert events.index("audit") > max(index for index, event in enumerate(events) if event == "index")
        assert events.index("audit") < events.index("live-lock") < events.index("swap") < events.index("terminal")
    else:
        assert events == ["begin", "stage-lock", "audit", "terminal", "commit"]


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

    database = SimpleNamespace(transaction=transaction, status=AsyncMock(return_value=int(can_claim)),
                               scalar=AsyncMock(side_effect=[11, 12, 21, 22]))
    if not can_claim:
        with pytest.raises(RuntimeError, match="claimable"):
            await ndc_stage.create_ndc_stages(database, attempt, standalone=is_standalone)
        assert not created_names
        assert events == ["rollback"]
        return
    await ndc_stage.create_ndc_stages(database, attempt, standalone=is_standalone)
    assert created_names == [f"product_{attempt.suffix}", f"package_{attempt.suffix}"]
    assert attempt.incumbent_oids == {"product": 11, "package": 12}
    assert attempt.table_oids == {"product": 21, "package": 22}
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
    assert database.status.call_args.kwargs == {"run_id": attempt.run_id, "attempt_id": attempt.suffix}


def test_attempt_names_are_private_unique_and_schema_is_not_executable():
    first = _attempt()
    second = _attempt()
    assert first.suffix != second.suffix
    assert len(first.suffix) == 33
    assert first.tables["product"].name != second.tables["product"].name
    with pytest.raises(ValueError, match="schema"):
        ndc_stage.new_ndc_attempt("synthetic-run", "public; DROP SCHEMA public")
