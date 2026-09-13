"""Disposable role-separated canonical views for local handoff tests."""

from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

from db.connection import Database
from db.models import Package, Product
from tests.process.test_ndc_publication_proof_postgres import _drop_schema


async def _drop_role(database, role_name):
    if await database.scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)", name=role_name):
        database_name = await database.scalar("SELECT quote_ident(current_database())")
        await database.status(f"REVOKE CREATE ON DATABASE {database_name} FROM {role_name}")
    await database.status(f"DROP ROLE IF EXISTS {role_name}")


async def _protect_canonical_pair(case, owner_name, reader_name):
    for model in (Product, Package):
        name = model.__tablename__
        columns = ", ".join('"' + column.name + '"' for column in model.__table__.columns)
        await case.database.status(f"ALTER TABLE {case.schema}.{name} RENAME TO {name}_generation")
        await case.database.status(f"ALTER TABLE {case.schema}.{name}_generation OWNER TO {owner_name}")
        await case.database.status(f"CREATE VIEW {case.schema}.{name} AS SELECT {columns} FROM {case.schema}.{name}_generation")
        await case.database.status(f"ALTER VIEW {case.schema}.{name} OWNER TO {owner_name}")
        await case.database.status(f"GRANT SELECT ON {case.schema}.{name} TO {reader_name}")
    await case.database.status(f"""
        CREATE UNIQUE INDEX import_run_active_idempotency_idx ON {case.schema}.import_run (idempotency_key)
         WHERE idempotency_key IS NOT NULL AND status IN ('queued', 'starting', 'running', 'finalizing', 'canceling')
    """)
    await case.database.status(f"ALTER TABLE {case.schema}.import_run OWNER TO {reader_name}")
    database_name = await case.database.scalar("SELECT quote_ident(current_database())")
    await case.database.status(f"GRANT CREATE ON DATABASE {database_name} TO {reader_name}")
    await case.database.status(f"ALTER SCHEMA {case.schema} OWNER TO {owner_name}")
    await case.database.status(f"GRANT USAGE, CREATE ON SCHEMA {case.schema} TO {reader_name}")


@asynccontextmanager
async def handoff_reader_case(case, monkeypatch):
    """Own each exact temporary role, schema and reader pool before creating it."""
    owner_name = "ndc_owner_" + uuid4().hex
    reader_name = "ndc_reader_" + uuid4().hex
    async with AsyncExitStack() as cleanup:
        for role_name in (owner_name, reader_name):
            cleanup.push_async_callback(_drop_role, case.database, role_name)
            login = "NOLOGIN" if role_name == owner_name else "LOGIN"
            await case.database.status(f"CREATE ROLE {role_name} {login} NOINHERIT NOSUPERUSER NOCREATEDB "
                                       "NOCREATEROLE NOREPLICATION NOBYPASSRLS")
        cleanup.push_async_callback(_drop_schema, case.database, case.schema)
        await _protect_canonical_pair(case, owner_name, reader_name)
        monkeypatch.setenv("HLTHPRT_DB_USER", reader_name)
        monkeypatch.setenv("DB_USER", reader_name)
        reader = Database()
        cleanup.push_async_callback(reader.disconnect)
        await reader.connect()
        yield SimpleNamespace(database=reader, schema=case.schema, reader_name=reader_name, owner_name=owner_name)
