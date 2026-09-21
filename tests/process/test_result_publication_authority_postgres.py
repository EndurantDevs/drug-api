import asyncio
import importlib
import importlib.util
import json
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

label_publish = importlib.import_module("process.label_publish")
authority = importlib.import_module("process.result_publication_authority")
drug_indications = importlib.import_module("process.drug_indications")
_DSN_ENV = "HLTHPRT_RESULT_AUTHORITY_TEST_DSN"
_DATABASE = re.compile(r"^drug_publication_authority_[0-9a-f]{32}$")
_PARAMETER = re.compile(r"(?<!:):([a-z_]+)")
_MIGRATION = Path(__file__).resolve().parents[2] / "alembic/versions/202609210001_add_result_publication_authority.py"


def _database_url():
    raw_url = os.getenv(_DSN_ENV, "")
    if not raw_url:
        pytest.skip(f"{_DSN_ENV} is required")
    database_url = make_url(raw_url)
    if (
        not database_url.drivername.startswith("postgresql")
        or database_url.host not in {"127.0.0.1", "localhost"}
        or database_url.port != 5440
        or not _DATABASE.fullmatch(str(database_url.database or ""))
    ):
        pytest.fail(f"{_DSN_ENV} must identify a task-owned local PostgreSQL database on port 5440")
    return database_url


def _migration_module():
    module_spec = importlib.util.spec_from_file_location("drug_result_authority_migration", _MIGRATION)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _run_migration(connection):
    migration = _migration_module()

    def upgrade(sync_connection):
        migration.op = Operations(MigrationContext.configure(sync_connection))
        migration.upgrade()

    upgrade(connection)


def _run_downgrade(engine):
    migration = _migration_module()
    try:
        with engine.begin() as connection:
            migration.op = Operations(MigrationContext.configure(connection))
            migration.downgrade()
    except RuntimeError as error:
        return str(error)
    return None


class _Database:
    def __init__(self, connection):
        self.connection = connection

    @asynccontextmanager
    async def transaction(self):
        async with self.connection.transaction():
            yield self

    async def status(self, statement, **parameters):
        query, values = _bound(statement, parameters)
        return await self.connection.execute(query, *values)

    async def first(self, statement, **parameters):
        query, values = _bound(statement, parameters)
        return await self.connection.fetchrow(query, *values)

    async def all(self, statement, **parameters):
        query, values = _bound(statement, parameters)
        return await self.connection.fetch(query, *values)


class _PoolDatabase(_Database):
    def __init__(self, pool):
        super().__init__(None)
        self.pool = pool

    @asynccontextmanager
    async def transaction(self):
        if self.connection is not None:
            yield self
            return
        async with self.pool.acquire() as connection, connection.transaction():
            self.connection = connection
            try:
                yield self
            finally:
                self.connection = None


def _bound(statement, parameters):
    values = []

    def replace(parameter_match):
        values.append(parameters[parameter_match.group(1)])
        return f"${len(values)}"

    return _PARAMETER.sub(replace, str(statement)), values


async def _create_label_table(connection, schema, table_name, generation):
    await connection.execute(
        f'CREATE TABLE "{schema}"."{table_name}" ('
        "id text, set_id text, product_ndc text[], package_ndc text[], generation text)"
    )
    await connection.execute(f'INSERT INTO "{schema}"."{table_name}" (generation) VALUES ($1)', generation)


async def _create_live_indexes(connection, schema):
    await connection.execute(f'CREATE INDEX idx_product_ndc ON "{schema}".label USING GIN(product_ndc)')
    await connection.execute(f'CREATE INDEX idx_package_ndc ON "{schema}".label USING GIN(package_ndc)')
    await connection.execute(f'CREATE INDEX idx_label_id ON "{schema}".label (id)')
    await connection.execute(f'CREATE INDEX idx_label_set_id ON "{schema}".label (set_id)')


async def _relation_state(connection, schema, table_name):
    relation_oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", f"{schema}.{table_name}"
    )
    if relation_oid is None:
        return None
    generation = await connection.fetchval(f'SELECT generation FROM "{schema}"."{table_name}"')
    return relation_oid, generation


async def _authority_state(connection, schema):
    return await connection.fetchrow(
        f'SELECT local_generation, origin_lineage_id, origin_generation, relation_oids, '
        f'consumed_dependencies FROM "{schema}".result_publication_authority WHERE importer_id=\'label\''
    )


async def _prepare_label_generations(connection, schema):
    async with connection.transaction():
        await _create_label_table(connection, schema, "label", "current")
        await _create_label_table(connection, schema, "label_old", "previous")
        await _create_label_table(connection, schema, "label_20260921", "incoming")
        await _create_live_indexes(connection, schema)
    with pytest.raises(asyncpg.CheckViolationError, match="result_publication_authority_shape_check"):
        async with connection.transaction():
            await connection.execute(
                f'UPDATE "{schema}".result_publication_authority SET local_generation=1 '
                "WHERE importer_id='label'"
            )


async def _assert_authority_constraint_rejects_nulls(connection, schema):
    invalid_updates = (
        (
            "label",
            "origin_generation=NULL, consumed_dependencies='{}'::jsonb",
        ),
        (
            "drug-indications",
            "origin_generation=1, consumed_dependencies="
            "'{\"label\":{},\"ndc\":{},\"clinical-reference\":{}}'::jsonb",
        ),
    )
    for importer_id, fields in invalid_updates:
        with pytest.raises(asyncpg.CheckViolationError, match="result_publication_authority_shape_check"):
            async with connection.transaction():
                await connection.execute(
                    f'UPDATE "{schema}".result_publication_authority SET local_generation=1, '
                    f"origin_lineage_id=local_lineage_id, {fields}, published_at=clock_timestamp(), "
                    "relation_oids=ARRAY[1]::bigint[] WHERE importer_id=$1",
                    importer_id,
                )


async def _wait_for_queued_lock(connection, schema, relation, mode):
    for _attempt in range(100):
        queued = await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_locks locks "
            "JOIN pg_class relations ON relations.oid=locks.relation "
            "JOIN pg_namespace schemas ON schemas.oid=relations.relnamespace "
            "WHERE schemas.nspname=$1 AND relations.relname=$2 AND locks.mode=$3 AND NOT locks.granted)",
            schema,
            relation,
            mode,
        )
        if queued:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"{mode} did not queue on {schema}.{relation}")


async def _take_exclusive_lock(connection, schema, relation):
    async with connection.transaction():
        await connection.execute(f'LOCK TABLE "{schema}"."{relation}" IN ACCESS EXCLUSIVE MODE')


def _create_dependency_tables(connection, schema):
    connection.execute(text(f'CREATE TABLE "{schema}".label (id text)'))
    connection.execute(text(f'CREATE TABLE "{schema}".product (id text)'))
    connection.execute(
        text(
            f'CREATE TABLE "{schema}".code_relationship ('
            "from_code text, to_system text, to_code text, relationship text, "
            "source_attribution text, from_system text)"
        )
    )
    connection.execute(
        text(
            f'CREATE TABLE "{schema}".code_catalog ('
            "code_system text, code text, code_type text, source_attribution text, display_name text)"
        )
    )
    connection.execute(
        text(
            f'CREATE TABLE "{schema}".code_synonym ('
            "code_system text, code text, synonym text, term_type text)"
        )
    )


async def _assert_clinical_share_lock_supported(connection, schema):
    for relation in (
        "label",
        "product",
        "code_relationship",
        "code_catalog",
        "code_synonym",
    ):
        assert await connection.fetchval(
            "SELECT has_table_privilege(current_user, $1, 'UPDATE')",
            f'"{schema}"."{relation}"',
        )
    relationship_rows, term_rows, clinical = await drug_indications._read_clinical_rows(
        connection, schema
    )
    assert relationship_rows == [] and term_rows == []
    assert [entry["name"] for entry in clinical["relations"]] == [
        "code_relationship",
        "code_catalog",
        "code_synonym",
    ]


async def _publish_and_verify_success(connection, schema):
    await label_publish.publish_label_table(_Database(connection), schema, "20260921")
    published_authority = await _authority_state(connection, schema)
    assert published_authority["local_generation"] == 1
    assert published_authority["origin_lineage_id"] is not None
    assert published_authority["origin_generation"] == 1
    assert published_authority["consumed_dependencies"] == {}
    assert await _relation_state(connection, schema, "label") == (
        published_authority["relation_oids"][0],
        "incoming",
    )
    assert (await _relation_state(connection, schema, "label_old"))[1] == "current"
    return published_authority


async def _verify_authority_failure_rolls_back(connection, schema, published_authority):
    async with connection.transaction():
        await _create_label_table(connection, schema, "label_20260922", "rejected")
        await connection.execute(
            f'ALTER TABLE "{schema}".result_publication_authority ADD CONSTRAINT '
            "result_publication_test_reject CHECK (local_generation < 2)"
        )
    state_by_table_before_failure = {
        table_name: await _relation_state(connection, schema, table_name)
        for table_name in ("label", "label_old", "label_20260922")
    }
    with pytest.raises(asyncpg.CheckViolationError, match="result_publication_test_reject"):
        await label_publish.publish_label_table(_Database(connection), schema, "20260922")
    assert {
        table_name: await _relation_state(connection, schema, table_name)
        for table_name in state_by_table_before_failure
    } == state_by_table_before_failure
    assert dict(await _authority_state(connection, schema)) == dict(published_authority)


@pytest.mark.asyncio
async def test_label_swap_and_authority_commit_or_roll_back_together(monkeypatch):
    """Prove authority and table rotation share one PostgreSQL transaction."""

    schema = "drug_result_authority_" + uuid4().hex
    monkeypatch.setenv("DB_SCHEMA", schema)
    database_url = _database_url()
    sync_engine = create_engine(database_url.set(drivername="postgresql+psycopg2"))
    connection = None
    try:
        with sync_engine.begin() as sync_connection:
            sync_connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            _run_migration(sync_connection)
        connection = await asyncpg.connect(
            database_url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        await connection.set_type_codec(
            "jsonb",
            encoder=lambda value: value if isinstance(value, str) else json.dumps(value),
            decoder=json.loads,
            schema="pg_catalog",
        )
        await _assert_authority_constraint_rejects_nulls(connection, schema)
        await _prepare_label_generations(connection, schema)
        published_authority = await _publish_and_verify_success(connection, schema)
        await _verify_authority_failure_rolls_back(connection, schema, published_authority)
    finally:
        if connection is not None:
            await connection.close()
        with sync_engine.begin() as sync_connection:
            sync_connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        sync_engine.dispose()


@pytest.mark.asyncio
async def test_dependency_snapshot_uses_one_pool_connection_and_queues_publishers(monkeypatch):
    """A size-one pool keeps the snapshot live while an exclusive publisher queues."""

    schema = "drug_result_authority_" + uuid4().hex
    monkeypatch.setenv("DB_SCHEMA", schema)
    database_url = _database_url()
    sync_engine = create_engine(database_url.set(drivername="postgresql+psycopg2"))
    pool = None
    publisher = None
    observer = None
    lock_task = None
    try:
        with sync_engine.begin() as sync_connection:
            sync_connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            _run_migration(sync_connection)
            _create_dependency_tables(sync_connection, schema)
        asyncpg_url = database_url.set(drivername="postgresql").render_as_string(hide_password=False)
        pool = await asyncpg.create_pool(asyncpg_url, min_size=1, max_size=1)
        database = _PoolDatabase(pool)
        publisher = await asyncpg.connect(asyncpg_url)
        observer = await asyncpg.connect(asyncpg_url)
        await _assert_clinical_share_lock_supported(observer, schema)

        async with database.transaction():
            label, ndc = await authority.local_indication_dependencies(database, schema)
            assert label["relations"][0]["name"] == "label"
            assert ndc["relations"][0]["name"] == "product"
            lock_task = asyncio.create_task(_take_exclusive_lock(publisher, schema, "label"))
            await _wait_for_queued_lock(observer, schema, "label", "AccessExclusiveLock")
            assert not lock_task.done()
        await lock_task
    finally:
        if lock_task is not None and not lock_task.done():
            lock_task.cancel()
            await asyncio.gather(lock_task, return_exceptions=True)
        for connection in (publisher, observer):
            if connection is not None:
                await connection.close()
        if pool is not None:
            await pool.close()
        with sync_engine.begin() as sync_connection:
            sync_connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        sync_engine.dispose()


@pytest.mark.asyncio
async def test_downgrade_waits_for_writer_then_refuses_committed_evidence(monkeypatch):
    """The downgrade fence cannot miss a concurrently committing generation."""

    schema = "drug_result_authority_" + uuid4().hex
    monkeypatch.setenv("DB_SCHEMA", schema)
    database_url = _database_url()
    sync_engine = create_engine(database_url.set(drivername="postgresql+psycopg2"))
    writer = None
    observer = None
    writer_transaction = None
    downgrade_task = None
    try:
        with sync_engine.begin() as sync_connection:
            sync_connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            _run_migration(sync_connection)
        asyncpg_url = database_url.set(drivername="postgresql").render_as_string(hide_password=False)
        writer = await asyncpg.connect(asyncpg_url)
        observer = await asyncpg.connect(asyncpg_url)
        writer_transaction = writer.transaction()
        await writer_transaction.start()
        await writer.execute(
            f'UPDATE "{schema}".result_publication_authority SET local_generation=1, '
            "origin_lineage_id=local_lineage_id, origin_generation=1, published_at=clock_timestamp(), "
            "relation_oids=ARRAY[1]::bigint[], consumed_dependencies='{}'::jsonb "
            "WHERE importer_id='label'"
        )
        downgrade_task = asyncio.create_task(asyncio.to_thread(_run_downgrade, sync_engine))
        await _wait_for_queued_lock(
            observer, schema, "result_publication_authority", "AccessExclusiveLock"
        )
        assert not downgrade_task.done()
        await writer_transaction.commit()
        writer_transaction = None

        assert await downgrade_task == "result publication evidence prevents downgrade"
        assert await observer.fetchval(
            f'SELECT local_generation FROM "{schema}".result_publication_authority '
            "WHERE importer_id='label'"
        ) == 1
    finally:
        if writer_transaction is not None:
            await writer_transaction.rollback()
        if downgrade_task is not None and not downgrade_task.done():
            await asyncio.gather(downgrade_task, return_exceptions=True)
        for connection in (writer, observer):
            if connection is not None:
                await connection.close()
        with sync_engine.begin() as sync_connection:
            sync_connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        sync_engine.dispose()
