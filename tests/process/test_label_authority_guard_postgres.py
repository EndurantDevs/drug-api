"""The database fences Label authority without blocking other native publications."""

import importlib.util
import json
from uuid import uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text

from db.drug_snapshot_runtime import publication
from tests.process.test_result_publication_authority_postgres import (
    _authority_state,
    _create_label_table,
    _Database,
    _database_url,
    _run_migration,
    label_publish,
)


def _guard_migration(connection, action="upgrade"):
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "alembic/versions/202609230001_guard_label_publication_authority.py"
    spec = importlib.util.spec_from_file_location("label_authority_guard_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration.op = Operations(MigrationContext.configure(connection))
    getattr(migration, action)()


async def _reject_runtime_writes(connection, schema):
    authority_before_map = dict(await _authority_state(connection, schema))
    statements = (
        f"UPDATE {schema}.result_publication_authority SET local_generation=local_generation+1 WHERE importer_id='label'",
        f"DELETE FROM {schema}.result_publication_authority WHERE importer_id='label'",
        f"INSERT INTO {schema}.result_publication_authority SELECT * FROM {schema}.result_publication_authority WHERE importer_id='label'",
        f"UPDATE {schema}.result_publication_authority SET importer_id='drug-indications' WHERE importer_id='label'",
        f"UPDATE {schema}.result_publication_authority SET importer_id='label' WHERE importer_id='drug-indications'",
        f"ALTER TABLE {schema}.result_publication_authority DISABLE TRIGGER label_publication_authority_owner",
        f"DROP TRIGGER label_publication_authority_owner ON {schema}.result_publication_authority",
        f"CREATE OR REPLACE FUNCTION {schema}.guard_label_publication_authority() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END;'",
        f"CREATE OR REPLACE TRIGGER label_publication_authority_owner BEFORE UPDATE ON {schema}.result_publication_authority FOR EACH ROW EXECUTE FUNCTION {schema}.guard_label_publication_authority()",
        f"TRUNCATE {schema}.result_publication_authority",
    )
    for statement in statements:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(statement)
    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="live table ownership"):
        async with connection.transaction():
            await publication.publish_local_result_generation(
                _Database(connection), importer_id="label", schema=schema, consumed_dependencies={}
            )
    assert dict(await _authority_state(connection, schema)) == authority_before_map


async def _indications_publish(connection, schema):
    dependencies = publication.indication_dependencies(
        publication.dependency_entry("label", [("label", 1)]),
        publication.dependency_entry("ndc", [("product", 2)]),
        publication.dependency_entry(
            "clinical-reference", [("code_relationship", 3), ("code_catalog", 4), ("code_synonym", 5)]
        ),
    )
    async with connection.transaction():
        observed = await publication.publish_local_result_generation(
            _Database(connection), importer_id="drug-indications", schema=schema, consumed_dependencies=dependencies
        )
    assert observed.local_generation > 0


async def _exercise_protected_publication(connection, engine, schema, runtime, owner, publisher, ordinary):
    await connection.execute(f'SET ROLE "{publisher}"')
    await connection.execute(f'ALTER TABLE "{schema}".label OWNER TO "{owner}"')
    await connection.execute(f'SET ROLE "{runtime}"')
    await _reject_runtime_writes(connection, schema)
    await _indications_publish(connection, schema)

    await connection.execute(f'SET ROLE "{publisher}"')
    incoming_map = {"origin_lineage_id": str(uuid4()), "origin_generation": 1, "published_at": "2026-09-23T00:00:00Z"}
    async with connection.transaction():
        adopted = await publication.adopt_label_generation(
            _Database(connection), schema=schema, source_generation=incoming_map
        )
    assert adopted.serving_generation.as_dict() == incoming_map
    with pytest.raises(RuntimeError, match="rollback probe"):
        async with connection.transaction():
            await publication.adopt_label_generation(
                _Database(connection), schema=schema, source_generation=ordinary.serving_generation
            )
            raise RuntimeError("rollback probe")
    assert (
        await publication.read_result_publication_authority(_Database(connection), importer_id="label", schema=schema)
    ) == adopted
    async with connection.transaction():
        restored = await publication.adopt_label_generation(
            _Database(connection), schema=schema, source_generation=ordinary.serving_generation
        )
    assert restored.serving_generation == ordinary.serving_generation
    with pytest.raises(RuntimeError, match="evidence prevents guard downgrade"):
        with engine.begin() as sync:
            _guard_migration(sync, "downgrade")
    await connection.execute(f'SET ROLE "{runtime}"')
    await _reject_runtime_writes(connection, schema)


@pytest.mark.asyncio
async def test_label_guard_enforces_ordinary_and_protected_publication(monkeypatch):
    """Real producer, protected SQL denial, publisher adoption and origin rollback."""
    identity = uuid4().hex
    schema = "label_guard_" + identity
    runtime = "writer_" + identity
    owner = "owner_" + identity
    publisher = "publisher_" + identity
    monkeypatch.setenv("DB_SCHEMA", schema)
    database_url = _database_url()
    engine = create_engine(database_url.set(drivername="postgresql+psycopg2"))
    connection = None
    created_roles = []
    try:
        with engine.begin() as sync:
            for role in (runtime, owner, publisher):
                sync.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
                created_roles.append(role)
            sync.execute(text(f'GRANT "{owner}", "{runtime}" TO "{publisher}"'))
            sync.execute(text(f'CREATE SCHEMA "{schema}"'))
            sync.execute(text(f'GRANT USAGE, CREATE ON SCHEMA "{schema}" TO "{runtime}", "{owner}", "{publisher}"'))
            _run_migration(sync)
            _guard_migration(sync)
            _guard_migration(sync, "downgrade")
            _guard_migration(sync)
            sync.execute(text(f'ALTER TABLE "{schema}".result_publication_authority OWNER TO "{owner}"'))
            sync.execute(text(f'ALTER FUNCTION "{schema}".guard_label_publication_authority() OWNER TO "{owner}"'))
            # INSERT/DELETE are deliberately granted to exercise trigger rejection,
            # while the protected owner fences trigger changes and truncation.
            sync.execute(
                text(f'GRANT SELECT, INSERT, UPDATE, DELETE ON "{schema}".result_publication_authority TO "{runtime}"')
            )
        connection = await asyncpg.connect(
            database_url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        await connection.set_type_codec(
            "jsonb",
            schema="pg_catalog",
            encoder=lambda value: value if isinstance(value, str) else json.dumps(value),
            decoder=json.loads,
        )
        await connection.execute(f'SET ROLE "{runtime}"')
        await _create_label_table(connection, schema, "label_20260923", "ordinary")
        await connection.execute(f'CREATE TABLE "{schema}".drug_condition_evidence (id text)')
        await label_publish.publish_label_table(_Database(connection), schema, "20260923")
        ordinary = await publication.read_result_publication_authority(
            _Database(connection), importer_id="label", schema=schema
        )
        assert ordinary.local_generation == 1
        await _indications_publish(connection, schema)

        await _exercise_protected_publication(connection, engine, schema, runtime, owner, publisher, ordinary)
    finally:
        if connection is not None:
            await connection.close()
        with engine.begin() as sync:
            sync.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            for role in reversed(created_roles):
                sync.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
        engine.dispose()
