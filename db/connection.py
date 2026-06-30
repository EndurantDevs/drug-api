from __future__ import annotations

import contextvars
import inspect
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional, Tuple

from sanic.exceptions import NotFound
from sqlalchemy import delete as sa_delete
from sqlalchemy import insert as sa_insert
from sqlalchemy import select as sa_select
from sqlalchemy import text as sa_text
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.schema import Table as SATable
from sqlalchemy.sql import Executable

from db.statement_adapters import (DeleteAdapter, FuncProxy, InsertAdapter, SelectAdapter, StatementAdapter,
                                   UpdateAdapter)


class QueryDescriptor:
    def __get__(self, _instance: Any, owner: type["ModelBase"]):
        return db.select(owner)


class ModelBase(DeclarativeBase):
    """Declarative model base with db shortcut helpers."""

    __abstract__ = True

    query = QueryDescriptor()

    @classmethod
    def insert(cls):
        """Build an insert statement for this model."""
        return db.insert(cls)

    @classmethod
    def select(cls, *columns: Any):
        """Build a select statement for this model or selected columns."""
        if not columns:
            return db.select(cls)
        resolved_columns = tuple(getattr(cls, column) if isinstance(column, str) else column for column in columns)
        return db.select(*resolved_columns)

    @classmethod
    def load(cls, *_columns: Any):
        """Build a select statement for this whole model."""
        return db.select(cls)

    @classmethod
    async def get(cls, *identity: Any):
        """Load one row by primary-key identity."""
        key = identity[0] if len(identity) == 1 else tuple(identity)
        async with db.session() as session:
            return await session.get(cls, key)

    @classmethod
    async def get_or_404(cls, *identity: Any):
        """Load one row by identity or raise Sanic NotFound."""
        row = await cls.get(*identity)
        if row is None:
            raise NotFound(f"{cls.__name__} is not found")
        return row


Base = ModelBase


def _coerce_columns(columns: Tuple[Any, ...]) -> Tuple[Any, ...]:
    if len(columns) == 1 and isinstance(columns[0], (list, tuple, set)):
        columns = tuple(columns[0])
    return columns


class ConnectionProxy:
    def __init__(self, connection):
        self._connection = connection
        self.raw_connection = None

    async def all(self, stmt: Any, **params: Any):
        """Execute a statement and return all mapping rows."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return result.mappings().all()

    async def first(self, stmt: Any, **params: Any):
        """Execute a statement and return the first mapping row."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return result.mappings().first()

    async def scalar(self, stmt: Any, **params: Any):
        """Execute a statement and return its first scalar value."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return result.scalar()

    async def status(self, stmt: Any, **params: Any):
        """Execute a statement and return its affected row count."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return getattr(result, "rowcount", None)

    @asynccontextmanager
    async def transaction(self):
        """Expose the current connection as a transaction-like context."""
        yield self

    async def close(self):
        """Satisfy the async connection interface without closing ownership."""
        return None


_SESSION: contextvars.ContextVar[AsyncSession] = contextvars.ContextVar("db_session")


def current_session() -> AsyncSession:
    """Return the SQLAlchemy session bound to the current context."""
    try:
        return _SESSION.get()
    except LookupError as exc:
        raise RuntimeError("No SQLAlchemy session bound to the current context") from exc


def _current_session_or_none() -> AsyncSession | None:
    try:
        return _SESSION.get()
    except LookupError:
        return None


def _is_env_enabled(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "on", "yes"}


@dataclass
class Database:
    engine: Any = None
    session_factory: Any = None
    func: FuncProxy = field(init=False, repr=False)

    Model = Base
    text = staticmethod(sa_text)
    metadata = Base.metadata

    def __post_init__(self) -> None:
        self.func = FuncProxy()

    async def connect(self) -> None:
        """Initialize the async SQLAlchemy engine and session factory."""
        if self.engine is not None:
            return

        driver = os.getenv("HLTHPRT_DB_DRIVER") or os.getenv("DB_DRIVER") or "postgresql+asyncpg"
        if driver == "asyncpg":
            driver = "postgresql+asyncpg"
        if driver != "postgresql+asyncpg":
            raise RuntimeError(f"drug-api requires SQLAlchemy asyncpg driver, got {driver!r}")

        url = URL.create(
            drivername=driver,
            username=os.getenv("HLTHPRT_DB_USER") or os.getenv("DB_USER") or "postgres",
            password=os.getenv("HLTHPRT_DB_PASSWORD") or os.getenv("DB_PASSWORD") or "",
            host=os.getenv("HLTHPRT_DB_HOST") or os.getenv("DB_HOST") or "127.0.0.1",
            port=int(os.getenv("HLTHPRT_DB_PORT") or os.getenv("DB_PORT") or "5432"),
            database=os.getenv("HLTHPRT_DB_DATABASE") or os.getenv("DB_DATABASE") or "postgres",
        )

        pool_min = int(os.getenv("HLTHPRT_DB_POOL_MIN_SIZE") or os.getenv("DB_POOL_MIN_SIZE") or "1")
        pool_max = int(os.getenv("HLTHPRT_DB_POOL_MAX_SIZE") or os.getenv("DB_POOL_MAX_SIZE") or "10")
        pool_size = max(pool_min, 1)
        max_overflow = max(pool_max - pool_size, 0)

        self.engine = create_async_engine(
            url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=_is_env_enabled(os.getenv("HLTHPRT_DB_ECHO") or os.getenv("DB_ECHO")),
            pool_pre_ping=True,
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            autoflush=False,
        )

    async def disconnect(self) -> None:
        """Dispose the async engine and clear the session factory."""
        if self.engine is None:
            return
        await self.engine.dispose()
        self.engine = None
        self.session_factory = None

    def select(self, *columns: Any):
        """Build a select adapter for the requested columns or model."""
        columns = _coerce_columns(columns)
        return SelectAdapter(self, sa_select(*columns))

    def insert(self, *args: Any, **kwargs: Any):
        """Build an insert adapter, using PostgreSQL insert for ORM tables."""
        target = args[0] if args else None
        if isinstance(target, SATable) or hasattr(target, "__table__"):
            if hasattr(target, "__table__"):
                args = (target.__table__,) + args[1:]
            return InsertAdapter(self, pg_insert(*args, **kwargs))
        return InsertAdapter(self, sa_insert(*args, **kwargs))

    def update(self, *args: Any, **kwargs: Any):
        """Build an update adapter."""
        return UpdateAdapter(self, sa_update(*args, **kwargs))

    def delete(self, *args: Any, **kwargs: Any):
        """Build a delete adapter."""
        return DeleteAdapter(self, sa_delete(*args, **kwargs))

    async def execute(self, stmt: Any, **params: Any):
        """Execute a statement inside a managed session."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        async with self.session() as session:
            return await session.execute(stmt, params)

    async def all(self, stmt: Any, **params: Any):
        """Execute a statement and return all mapping rows."""
        result = await self.execute(stmt, **params)
        return result.mappings().all()

    async def first(self, stmt: Any, **params: Any):
        """Execute a statement and return its first mapping row."""
        result = await self.execute(stmt, **params)
        return result.mappings().first()

    async def scalar(self, stmt: Any, **params: Any):
        """Execute a statement and return its first scalar value."""
        result = await self.execute(stmt, **params)
        return result.scalar()

    async def status(self, stmt: Any, **params: Any):
        """Execute a statement and return its affected row count."""
        result = await self.execute(stmt, **params)
        return getattr(result, "rowcount", None)

    async def create_table(self, table: SATable, **kwargs: Any) -> None:
        """Create a table and its schema when needed."""
        if self.engine is None:
            await self.connect()
        assert self.engine is not None
        async with self.engine.begin() as connection:
            if table.schema:
                preparer = connection.dialect.identifier_preparer
                await connection.exec_driver_sql(
                    f"CREATE SCHEMA IF NOT EXISTS {preparer.quote_schema(table.schema)}"
                )
            await connection.run_sync(table.create, **kwargs)

    async def execute_ddl(self, statement: str) -> None:
        """Execute raw DDL using autocommit."""
        if self.engine is None:
            await self.connect()
        assert self.engine is not None
        async with self.engine.connect() as connection:
            autocommit_conn = connection.execution_options(isolation_level="AUTOCOMMIT")
            if inspect.isawaitable(autocommit_conn):
                autocommit_conn = await autocommit_conn
            await autocommit_conn.exec_driver_sql(statement)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield the active context session or create a managed one."""
        existing_session = _current_session_or_none()
        if existing_session is not None:
            yield existing_session
            return

        if self.session_factory is None:
            await self.connect()
        assert self.session_factory is not None
        session = self.session_factory()
        token = _SESSION.set(session)
        try:
            yield session
            if session.in_transaction():
                await session.commit()
        except Exception:
            if session.in_transaction():
                await session.rollback()
            raise
        finally:
            await session.close()
            _SESSION.reset(token)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """Yield a session inside a transaction when one is not already active."""
        async with self.session() as session:
            if session.in_transaction():
                yield session
            else:
                async with session.begin():
                    yield session

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[ConnectionProxy]:
        """Yield a connection proxy backed by an engine transaction."""
        if self.engine is None:
            await self.connect()
        assert self.engine is not None
        async with self.engine.begin() as connection:
            yield ConnectionProxy(connection)

    def init_app(self, app) -> None:
        """Register Sanic lifecycle hooks and per-request session middleware."""
        @app.listener("after_server_start")
        async def _on_start(_, __):
            await self.connect()

        @app.listener("before_server_stop")
        async def _on_stop(_, __):
            await self.disconnect()

        @app.middleware("request")
        async def _bind_session(request):
            if self.session_factory is None:
                await self.connect()
            assert self.session_factory is not None
            session = self.session_factory()
            token = _SESSION.set(session)
            request.ctx.sa_session = session
            request.ctx.session = session
            request.ctx._sa_session_token = token

        @app.middleware("response")
        async def _cleanup_session(request, response):
            session = getattr(request.ctx, "sa_session", None)
            token = getattr(request.ctx, "_sa_session_token", None)
            if session is None:
                return
            try:
                if session.in_transaction():
                    status = getattr(response, "status", 500) if response is not None else 500
                    if status < 400:
                        await session.commit()
                    else:
                        await session.rollback()
            except SQLAlchemyError:
                await session.rollback()
                raise
            finally:
                await session.close()
                if token is not None:
                    _SESSION.reset(token)


db = Database()


async def init_db(_: Any = None, loop: Any = None) -> None:
    """Initialize the module-level database connection."""
    del loop
    await db.connect()


__all__ = [
    "Base",
    "Database",
    "StatementAdapter",
    "SelectAdapter",
    "InsertAdapter",
    "UpdateAdapter",
    "DeleteAdapter",
    "ConnectionProxy",
    "current_session",
    "db",
    "init_db",
]
