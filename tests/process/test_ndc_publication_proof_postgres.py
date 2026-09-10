"""Native NDC publication evidence from actual owned stages and paired saves.

Stage cases use synthetic acquisition metadata; the coordinator parses controlled
manifest and ZIP bytes. Every publication receipt is produced by native audit.
No external acquisition or control-plane acceptance is claimed.
"""

import datetime
import hashlib
import json
import os
from contextlib import AsyncExitStack
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, insert, text
from sqlalchemy.exc import SQLAlchemyError

from api import control_imports, control_run_store
from db.connection import Database
from db.models import Package, Product
from process import control_lifecycle, ndc_product, ndc_publish, ndc_stage
from tests.process.ndc_publication_fixtures import (
    copy_publication_pair_bytes,
    install_coordinator_sources,
    non_iso_publication_connection,
)

_MODELS = {"product": Product, "package": Package}


async def _drop_schema(database, schema):
    await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    assert await database.scalar("SELECT to_regnamespace(:schema)", schema=schema) is None


@pytest.fixture
async def publication_case():
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if os.getenv("HLTHPRT_ENVIRONMENT") != "test" or not (
        "test" in database_name or database_name.endswith("_ci")
    ):
        pytest.skip("requires an explicitly configured disposable PostgreSQL test database")
    database = Database()
    schema = "ndc_proof_test_" + uuid4().hex
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(database.disconnect)
        await database.connect()
        for extension in ("pg_trgm", "btree_gin"):
            await database.status(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        cleanup.push_async_callback(_drop_schema, database, schema)
        await database.status(f"CREATE SCHEMA {schema}")
        await _create_run_table(database, schema)
        for suffix, ordinal in (("", 0), ("_old", 9)):
            await _seed_pair(database, schema, suffix, ordinal)
        attempt = ndc_stage.new_ndc_attempt("synthetic-ndc-" + uuid4().hex, schema)
        logical_import_id = "synthetic-logical-" + uuid4().hex
        await database.status(f"""
            INSERT INTO {schema}.import_run (run_id, engine, importer, family, status, import_id)
            VALUES (:run_id, 'drug-api', 'ndc', 'drug', 'queued', :import_id)
        """, run_id=attempt.run_id, import_id=logical_import_id)
        case = SimpleNamespace(database=database, schema=schema, attempt=attempt, logical_import_id=logical_import_id)
        case.queued_run = await _run_state(case)
        await ndc_stage.create_ndc_stages(database, attempt, standalone=False)
        yield case


async def _create_run_table(database, schema):
    await database.status(f"""
        CREATE TABLE {schema}.import_run (
            run_id VARCHAR PRIMARY KEY, engine VARCHAR NOT NULL, node_id VARCHAR,
            importer VARCHAR NOT NULL, family VARCHAR, status VARCHAR NOT NULL,
            phase_detail TEXT, params JSONB DEFAULT '{{}}'::jsonb,
            idempotency_key VARCHAR, triggered_by VARCHAR, schedule_id VARCHAR,
            created_at TIMESTAMP, started_at TIMESTAMP, heartbeat_at TIMESTAMP,
            finished_at TIMESTAMP, progress JSONB DEFAULT '{{}}'::jsonb,
            metrics JSONB DEFAULT '{{}}'::jsonb, error JSONB, import_id VARCHAR,
            retry_of_run_id VARCHAR
        )
    """)


def _paired_rows(ordinal):
    product_row_dict = {column.name: None for column in Product.__table__.columns}
    product_row_dict.update(product_id=f"synthetic-product-{ordinal}", product_ndc=f"90000-000{ordinal}",
                       generic_name="Synthetic generic", brand_name="Synthetic brand", labeler_name="Synthetic labeler",
                       active_ingredients=[{"name": "Synthetic ingredient", "strength": "1 mg"}],
                       finished=True, openfda={"rxcui": [str(ordinal + 1)]}, rxnorm_ids=[str(ordinal + 1)],
                       route=["ORAL"], pharm_class=[], marketing_start_date=datetime.date(2020, 1, 1))
    package_row_dict = {column.name: None for column in Package.__table__.columns}
    package_row_dict.update(package_ndc=f"90000-000{ordinal}-01", product_ndc=product_row_dict["product_ndc"],
                       ndc11=f"90000000{ordinal}01", description="1 bottle", size=1, packages_number=1,
                       sample=False, marketing_start_date=datetime.date(2020, 1, 1))
    return product_row_dict, package_row_dict


async def _seed_pair(database, schema, suffix, ordinal):
    metadata = MetaData()
    async with database.transaction() as session:
        connection = await session.connection()
        for model, row in zip((Product, Package), _paired_rows(ordinal), strict=True):
            table = model.__table__.to_metadata(metadata, schema=schema, name=model.__tablename__ + suffix)
            await connection.run_sync(table.create)
            await session.execute(insert(table).values(row))


async def _save_one(case, ordinal=1):
    product_row_dict, package_row_dict = _paired_rows(ordinal)
    await ndc_stage.save_ndc_batch(case.database, case.attempt, [product_row_dict], [package_row_dict])
    return product_row_dict, package_row_dict


def _acquisition(case, *, complete=True):
    records = case.attempt.counts["source_products"]
    source_records = records if complete else records + 100
    partitions = [{"file": "https://example.test/ndc/part-1.json.zip", "records": source_records}]
    section_dict = {"export_date": "2026-01-01", "total_records": source_records, "partitions": partitions}
    return {"complete": complete, "source_records": source_records, "export_date": section_dict["export_date"],
            "ndc_section_sha256": _source_sha256(section_dict),
            "selected_partitions_sha256": _source_sha256(partitions),
            "manifest": {"sha256": "a" * 64, "size_bytes": 128}, "partitions": [{
                "requested_url": partitions[0]["file"],
                "sha256": "b" * 64, "size_bytes": 256, "records": records,
                "declared_records": source_records,
            }]}


def _source_sha256(value):
    canonical_bytes = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    return hashlib.sha256(canonical_bytes).hexdigest()


async def _publish(case, *, complete=True):
    return await ndc_publish.publish_ndc_tables(case.database, case.schema, case.attempt.suffix,
                                               attempt=case.attempt, acquisition=_acquisition(case, complete=complete))


async def _run_state(case):
    return dict(await case.database.first(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run_id",
                                          run_id=case.attempt.run_id))


async def _pair_state(case, suffix=""):
    return {name: await _table_state(case.database, case.schema, name, suffix) for name in _MODELS}


async def _table_state(database, schema, name, suffix):
    relation = f"{schema}.{name}{suffix}"
    primary_key = "product_id" if name == "product" else "package_ndc"
    columns = ", ".join('"' + column.name + '"' for column in _MODELS[name].__table__.columns)
    chunks = []

    async def collect(chunk):
        chunks.append(bytes(chunk))

    async with database.engine.connect() as connection:
        raw_connection = await connection.get_raw_connection()
        completion = await raw_connection.driver_connection.copy_from_query(
            f'SELECT {columns} FROM {relation} ORDER BY "{primary_key}" COLLATE "C"',
            output=collect, format="csv", timeout=5,
        )
    payload = b"".join(chunks)
    row_count = await database.scalar(f"SELECT count(*) FROM {relation}")
    assert completion == f"COPY {row_count}"
    table_oid = await database.scalar("SELECT to_regclass(:relation)::oid", relation=relation)
    return {"oid": table_oid, "row_count": row_count, "sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload), "comment": await database.scalar(
                "SELECT obj_description(:oid, 'pg_class')", oid=table_oid),
            "row_versions": tuple((await database.execute(
                f'SELECT xmin::text, ctid::text FROM {relation} ORDER BY "{primary_key}" COLLATE "C"')).all()),
            "indexes": tuple((await database.execute(
                "SELECT c.oid, c.relname, i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                "WHERE i.indrelid=:oid ORDER BY c.relname", oid=table_oid)).all())}


@pytest.mark.asyncio
async def test_owned_stages_and_identical_replay(publication_case):
    case = publication_case
    run = await _run_state(case)
    assert run["status"] == "running" and run["import_id"] == case.logical_import_id
    assert run["metrics"] == {"ndc_attempt_id": case.attempt.suffix}
    assert case.attempt.incumbent_oids == {name: state["oid"] for name, state in (await _pair_state(case)).items()}
    await _save_one(case)
    before = await _pair_state(case, "_" + case.attempt.suffix)
    assert all(state["comment"] == case.attempt.owner_comment for state in before.values())
    await _save_one(case)
    assert await _pair_state(case, "_" + case.attempt.suffix) == before
    assert case.attempt.counts == {"source_products": 2, "source_packages": 2, "product": 1, "package": 1, "batches": 2}
    assert (await _run_state(case))["import_id"] == case.logical_import_id


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["stored-conflict", "batch-conflict", "sql-error"])
async def test_paired_save_failure_rolls_back_product(publication_case, failure):
    case = publication_case
    _, prior_package = await _save_one(case)
    before = await _pair_state(case, "_" + case.attempt.suffix)
    counts_before = deepcopy(case.attempt.counts)
    product_row_dict, package_row_dict = _paired_rows(2)
    packages = [package_row_dict]
    expected_error = SQLAlchemyError
    if failure == "stored-conflict":
        packages = [{**prior_package, "description": "conflicting description"}]
        expected_error = RuntimeError
    elif failure == "batch-conflict":
        packages.append({**package_row_dict, "description": "conflicting description"})
        expected_error = ValueError
    else:
        package_row_dict["size"] = "not-an-integer"
    with pytest.raises(expected_error):
        await ndc_stage.save_ndc_batch(case.database, case.attempt, [product_row_dict], packages)
    assert await _pair_state(case, "_" + case.attempt.suffix) == before
    assert case.attempt.counts == counts_before
    assert (await _run_state(case))["status"] == "running"


@pytest.mark.asyncio
async def test_publication_receipt_binds_actual_pair(publication_case):
    case = publication_case
    await _save_one(case)
    assert (await _run_state(case))["import_id"] == case.logical_import_id
    before = await _pair_state(case)
    staged = await _pair_state(case, "_" + case.attempt.suffix)
    receipt = await _publish(case)
    current = await _pair_state(case)
    run = await _run_state(case)
    assert receipt["published"] is True and receipt["complete"] is True
    assert receipt["run_id"] == case.attempt.run_id and receipt["attempt_id"] == case.attempt.suffix
    assert receipt["acquisition"] == _acquisition(case) and receipt["counts"] == case.attempt.counts
    assert datetime.datetime.fromisoformat(receipt["completed_at"]).tzinfo is not None
    assert run["status"] == "succeeded" and run["phase_detail"] == "ndc import published"
    assert run["import_id"] == case.logical_import_id
    assert run["metrics"] == {"ndc_attempt_id": case.attempt.suffix, "ndc_publication": receipt,
                              "source_product_count": 1, "imported_product_count": 1}
    assert run["finished_at"] is not None and run["error"] is None
    for name in _MODELS:
        expected_table_dict = {key: current[name][key] for key in ("oid", "sha256", "size_bytes", "row_count")}
        assert {key: receipt["tables"][name][key] for key in expected_table_dict} == expected_table_dict
        assert current[name]["oid"] == staged[name]["oid"]
        assert json.loads(current[name]["comment"]) == receipt
        assert receipt["tables"][name]["encoding"] == "postgres-copy-csv-v1"
        assert [column["name"] for column in receipt["tables"][name]["columns"]] == list(_MODELS[name].__table__.columns.keys())
        assert await case.database.scalar("SELECT to_regclass(:name)", name=f"{case.schema}.{name}_{case.attempt.suffix}") is None
    previous = await _pair_state(case, "_old")
    assert {name: previous[name]["oid"] for name in _MODELS} == {name: before[name]["oid"] for name in _MODELS}


@pytest.mark.asyncio
async def test_publication_audit_uses_transaction_local_iso_dates(publication_case, monkeypatch):
    case = publication_case
    await _save_one(case)
    async with non_iso_publication_connection(case.database, monkeypatch) as connection:
        assert await case.database.scalar("SHOW DateStyle") == "SQL, DMY"
        receipt = await _publish(case)
        assert receipt["published"] is True
        assert await connection.scalar(text("SHOW DateStyle")) == "SQL, DMY"
        non_iso_by_table = await copy_publication_pair_bytes(connection, case.schema)
        await connection.execute(text("SET LOCAL DateStyle TO 'ISO, YMD'"))
        canonical_by_table = await copy_publication_pair_bytes(connection, case.schema)
        for name, canonical_bytes in canonical_by_table.items():
            assert b"2020-01-01" in canonical_bytes and b"01/01/2020" in non_iso_by_table[name]
            assert canonical_bytes != non_iso_by_table[name]
            assert receipt["tables"][name]["sha256"] == hashlib.sha256(canonical_bytes).hexdigest()
            assert receipt["tables"][name]["size_bytes"] == len(canonical_bytes)
        await connection.commit()
        assert await connection.scalar(text("SHOW DateStyle")) == "SQL, DMY"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["second-swap", "terminal-cas"])
async def test_publication_failure_retains_both_generations(publication_case, monkeypatch, failure):
    case = publication_case
    await _save_one(case)
    before_by_suffix = {suffix: await _pair_state(case, suffix) for suffix in ("", "_old", "_" + case.attempt.suffix)}
    if failure == "second-swap":
        original = ndc_publish._publish_single_ndc_table

        async def fail_second(database, schema, table, suffix):
            if table == "package":
                assert await database.scalar("SELECT to_regclass(:name)::oid", name=f"{schema}.product") == case.attempt.table_oids["product"]
                raise RuntimeError("synthetic second swap failure")
            await original(database, schema, table, suffix)

        monkeypatch.setattr(ndc_publish, "_publish_single_ndc_table", fail_second)
        expected_message = "synthetic second swap failure"
    else:
        original = ndc_publish.finish_ndc_publication

        async def lose_terminal_cas(database, attempt, receipt, *, published):
            assert published and set(receipt["tables"]) == {"product", "package"}
            async with database.engine.begin() as independent:
                await independent.execute(text("SET LOCAL statement_timeout='1000ms'"))
                changed = await independent.execute(text(f"UPDATE {case.schema}.import_run SET status='canceled' WHERE run_id=:run_id"),
                                                    {"run_id": attempt.run_id})
                assert changed.rowcount == 1
            return await original(database, attempt, receipt, published=published)

        monkeypatch.setattr(ndc_publish, "finish_ndc_publication", lose_terminal_cas)
        expected_message = "terminal success lost"
    with pytest.raises(RuntimeError, match=expected_message):
        await _publish(case)
    for suffix, expected in before_by_suffix.items():
        assert await _pair_state(case, suffix) == expected
    run = await _run_state(case)
    assert run["status"] == ("canceled" if failure == "terminal-cas" else "running")
    assert run["metrics"] == {"ndc_attempt_id": case.attempt.suffix}
    assert run["finished_at"] is None and run["error"] is None


@pytest.mark.asyncio
async def test_sample_discards_stages_and_preserves_incumbents(publication_case):
    case = publication_case
    await _save_one(case)
    before = await _pair_state(case)
    previous = await _pair_state(case, "_old")
    receipt = await _publish(case, complete=False)
    assert receipt["complete"] is False and receipt["published"] is False
    assert await _pair_state(case) == before and await _pair_state(case, "_old") == previous
    assert await ndc_stage.ndc_table_oids(case.database, case.schema, case.attempt.suffix) == {
        "product": None, "package": None,
    }
    run = await _run_state(case)
    assert run["status"] == "succeeded" and run["phase_detail"] == "ndc sample validated"
    assert run["metrics"] == {"ndc_attempt_id": case.attempt.suffix, "ndc_sample": receipt,
                              "source_product_count": 1, "imported_product_count": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign_owner", [False, True])
async def test_failed_attempt_cleanup_preserves_live_pair_and_foreign_stages(publication_case, foreign_owner):
    case = publication_case
    await _save_one(case)
    before = await _pair_state(case)
    previous = await _pair_state(case, "_old")
    assert await ndc_stage.fail_ndc_attempt(case.database, case.attempt) == 1
    failed_run = await _run_state(case)
    if foreign_owner:
        await ndc_stage._comment_ndc_table(
            case.database, case.schema, case.attempt.tables["package"].name, "foreign owner")
        staged = await _pair_state(case, "_" + case.attempt.suffix)
        with pytest.raises(RuntimeError, match="ownership changed"):
            await ndc_stage.discard_ndc_stages(case.database, case.attempt)
        assert await _pair_state(case, "_" + case.attempt.suffix) == staged
    else:
        await ndc_stage.discard_ndc_stages(case.database, case.attempt)
        await ndc_stage.discard_ndc_stages(case.database, case.attempt)
        assert await ndc_stage.ndc_table_oids(case.database, case.schema, case.attempt.suffix) == {
            "product": None, "package": None,
        }
    assert await _pair_state(case) == before and await _pair_state(case, "_old") == previous
    assert await _run_state(case) == failed_run


@pytest.mark.asyncio
async def test_changed_incumbent_rejects_publication(publication_case):
    case = publication_case
    await _save_one(case)
    replacement = Package.__table__.to_metadata(MetaData(), schema=case.schema, name="package_replacement")
    async with case.database.transaction() as session:
        connection = await session.connection()
        await connection.run_sync(replacement.create)
        await session.execute(insert(replacement).values(_paired_rows(3)[1]))
        await case.database.status(f"ALTER TABLE {case.schema}.package RENAME TO package_external_previous")
        await case.database.status(f"ALTER TABLE {case.schema}.package_replacement RENAME TO package")
    before_by_suffix = {suffix: await _pair_state(case, suffix) for suffix in ("", "_old", "_" + case.attempt.suffix)}
    with pytest.raises(RuntimeError, match="incumbent changed"):
        await _publish(case)
    for suffix, expected in before_by_suffix.items():
        assert await _pair_state(case, suffix) == expected
    run = await _run_state(case)
    assert run["status"] == "running" and run["metrics"] == {"ndc_attempt_id": case.attempt.suffix}
    assert run["finished_at"] is None


@pytest.mark.asyncio
async def test_stale_queue_writers_preserve_published_receipt(publication_case, monkeypatch):
    case = publication_case
    await _save_one(case)
    receipt = await _publish(case)
    before = await _run_state(case)
    serving_before = await _pair_state(case)
    monkeypatch.setattr(control_run_store, "db", case.database)
    monkeypatch.setattr(control_imports, "db", case.database)
    monkeypatch.setattr(control_imports, "_schema", lambda: case.schema)
    enqueue_update_dict = {"status": "queued", "phase_detail": "enqueued", "heartbeat_at": datetime.datetime(2020, 1, 1),
                      "progress": {}, "metrics": {"queue": "synthetic-queue", "job_id": "synthetic-job"}, "error": None}
    assert await control_run_store.update_import_run_after_enqueue(case.schema, case.attempt.run_id, enqueue_update_dict) == 0
    assert await _run_state(case) == before

    read_current = control_imports.get_import_run
    reads = []

    async def stale_then_current(run_id):
        reads.append(run_id)
        if len(reads) == 1:
            return deepcopy(case.queued_run)
        return await read_current(run_id)

    remove_job = AsyncMock(return_value={"redis": True, "removed": False})
    live_event = Mock()
    status_event = Mock()
    monkeypatch.setattr(control_imports, "get_import_run", stale_then_current)
    monkeypatch.setattr(control_imports, "read_live_progress", Mock(return_value=None))
    monkeypatch.setattr(control_imports, "_remove_queued_job", remove_job)
    monkeypatch.setattr(control_imports, "_write_run_live_progress", live_event)
    monkeypatch.setattr(control_imports, "enqueue_status_event", status_event)
    cancellation = await control_imports.request_cancel(case.attempt.run_id)
    assert reads == [case.attempt.run_id, case.attempt.run_id]
    remove_job.assert_awaited_once_with(case.queued_run)
    live_event.assert_not_called()
    status_event.assert_not_called()
    assert cancellation["status"] == "succeeded" and cancellation["metrics"]["ndc_publication"] == receipt
    assert cancellation["import_id"] == case.logical_import_id
    assert await _run_state(case) == before
    assert await _pair_state(case) == serving_before


@pytest.mark.asyncio
async def test_admission_commits_before_enqueue_with_bound_request_session(publication_case, monkeypatch):
    case = publication_case
    run_id = "synthetic-admission-" + uuid4().hex
    logical_import_id = "synthetic-admission-logical-" + uuid4().hex
    observed_rows = []
    monkeypatch.setattr(control_run_store, "db", case.database)
    monkeypatch.setattr(control_imports, "db", case.database)
    monkeypatch.setattr(control_imports, "_schema", lambda: case.schema)
    monkeypatch.setattr(control_imports, "enqueue_status_event", Mock())
    monkeypatch.setattr(control_imports, "_write_run_live_progress", Mock())

    async def enqueue_with_independent_reader(_spec, run_record_dict):
        async with case.database.session() as bound_session:
            assert bound_session is request_session
            request_backend = await case.database.scalar("SELECT pg_backend_pid()")
        async with case.database.engine.begin() as independent:
            await independent.execute(text("SET LOCAL statement_timeout='5s'"))
            assert await independent.scalar(text("SELECT pg_backend_pid()")) != request_backend
            rows = await independent.execute(text(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run_id"),
                                             {"run_id": run_id})
            queued_row = rows.mappings().one()
            assert queued_row["status"] == "queued" and queued_row["import_id"] == logical_import_id
            assert queued_row["metrics"] == {} and queued_row["run_id"] == run_record_dict["run_id"]
            observed_rows.append(dict(queued_row))
        return {"status": "queued", "phase_detail": "enqueued", "heartbeat_at": run_record_dict["heartbeat_at"],
                "progress": run_record_dict["progress"], "metrics": {"queue": "synthetic-queue"}, "error": None}

    monkeypatch.setattr(control_imports, "_enqueue", enqueue_with_independent_reader)
    async with case.database.session() as request_session:
        await case.database.scalar("SELECT pg_backend_pid()")
        admitted, created = await control_imports.create_import_run(
            {"run_id": run_id, "importer": "ndc", "import_id": logical_import_id})
        assert created and len(observed_rows) == 1
        assert admitted["status"] == "queued" and admitted["import_id"] == logical_import_id
    persisted = await case.database.first(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run_id", run_id=run_id)
    assert persisted["status"] == "queued" and persisted["import_id"] == logical_import_id
    assert persisted["metrics"] == {"queue": "synthetic-queue"}


@pytest.mark.asyncio
async def test_coordinator_publishes_acquired_pair_with_source_proof(publication_case, monkeypatch):
    case = publication_case
    run_id = "synthetic-coordinator-" + uuid4().hex
    logical_import_id = "synthetic-source-import-" + uuid4().hex
    await case.database.status(f"""
        INSERT INTO {case.schema}.import_run (run_id, engine, importer, status, import_id)
        VALUES (:run_id, 'drug-api', 'ndc', 'queued', :import_id)
    """, run_id=run_id, import_id=logical_import_id)
    manifest_url = "https://example.test/ndc/manifest.json"
    section_dict, payloads_by_url, temporary_paths = install_coordinator_sources(monkeypatch, manifest_url)
    monkeypatch.setattr(ndc_product, "db", case.database)
    monkeypatch.setattr(control_lifecycle, "db", case.database)
    monkeypatch.setattr(ndc_product, "enqueue_live_progress", Mock())
    monkeypatch.setattr(ndc_product, "enqueue_status_event", Mock())
    monkeypatch.setattr(control_lifecycle, "enqueue_live_progress", Mock())
    monkeypatch.setattr(control_lifecycle, "enqueue_status_event", Mock())
    monkeypatch.setenv("DB_SCHEMA", case.schema)
    monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", manifest_url)
    monkeypatch.setenv("SAVE_PER_PACK", "1")
    monkeypatch.setenv("HLTHPRT_NDC_PARTITION_CONCURRENCY", "2")
    before = await _pair_state(case)
    receipt = await ndc_product.init_file({}, {"run_id": run_id})
    run = await case.database.first(f"SELECT * FROM {case.schema}.import_run WHERE run_id=:run_id", run_id=run_id)
    assert run["status"] == "succeeded" and run["import_id"] == logical_import_id
    assert run["metrics"]["ndc_publication"] == receipt and run["metrics"]["ndc_attempt_id"] == receipt["attempt_id"]
    assert receipt["complete"] is True and receipt["published"] is True and receipt["run_id"] == run_id
    assert receipt["counts"] == {"source_products": 2, "source_packages": 2, "product": 2, "package": 2, "batches": 2}
    acquisition = receipt["acquisition"]
    assert acquisition["export_date"] == "2026-01-01" and acquisition["source_records"] == 2
    assert acquisition["ndc_section_sha256"] == _source_sha256(section_dict)
    assert acquisition["selected_partitions_sha256"] == _source_sha256(section_dict["partitions"])
    assert [part["requested_url"] for part in acquisition["partitions"]] == [part["file"] for part in section_dict["partitions"]]
    assert all(part["records"] == part["declared_records"] == 1 for part in acquisition["partitions"])
    for source_receipt in [acquisition["manifest"], *acquisition["partitions"]]:
        source_bytes = payloads_by_url[source_receipt["url"]]
        assert source_receipt["sha256"] == hashlib.sha256(source_bytes).hexdigest()
        assert source_receipt["size_bytes"] == len(source_bytes)
    current = await _pair_state(case)
    for name, state in current.items():
        assert state["row_count"] == 2 and state["oid"] != before[name]["oid"]
        assert json.loads(state["comment"]) == receipt
        for key in ("oid", "row_count", "sha256", "size_bytes"):
            assert receipt["tables"][name][key] == state[key]
    products = await case.database.all(f"SELECT product_id, short_dosage_form, rxnorm_ids FROM {case.schema}.product ORDER BY product_id")
    assert [tuple(table_row.values()) for table_row in products] == [("synthetic-source-1", "TABLET", ["1"]),
                                                       ("synthetic-source-2", "TABLET", ["2"])]
    packages = await case.database.all(f"SELECT ndc11, size, packages_number FROM {case.schema}.package ORDER BY ndc11")
    assert [tuple(table_row.values()) for table_row in packages] == [("90000000101", 30, 1), ("90000000201", 30, 1)]
    assert len(temporary_paths) == 3 and all(not path.exists() for path in temporary_paths)
