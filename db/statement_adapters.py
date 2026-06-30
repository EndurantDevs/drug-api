from __future__ import annotations

from typing import Any

from sqlalchemy import func as sa_func
from sqlalchemy.sql import Executable, Select
from sqlalchemy.sql.dml import Delete, Insert, Update


def _is_single_model_select(stmt: Executable) -> bool:
    try:
        descriptions = list(getattr(stmt, "column_descriptions", []) or [])
    except Exception:
        return False
    return len(descriptions) == 1 and descriptions[0].get("entity") is not None


class RowAdapter:
    def __init__(self, row: Any):
        self._row = row
        self._mapping = row._mapping

    def __getitem__(self, key: Any):
        if isinstance(key, int):
            return self._row[key]
        return self._mapping[key]

    def __iter__(self):
        return iter(self._row)

    def __len__(self) -> int:
        return len(self._row)

    def get(self, key: str, default: Any = None) -> Any:
        """Return a mapped value from the wrapped SQLAlchemy row."""
        return self._mapping.get(key, default)

    def keys(self):
        """Return row mapping keys."""
        return self._mapping.keys()

    def items(self):
        """Return row mapping items."""
        return self._mapping.items()

    def values(self):
        """Return row mapping values."""
        return self._mapping.values()


class StatementAdapter:
    def __init__(self, database: Any, stmt: Executable):
        self._db = database
        self._stmt = stmt

    def _wrap(self, stmt: Any):
        if isinstance(stmt, Select):
            return SelectAdapter(self._db, stmt)
        if isinstance(stmt, Insert):
            return InsertAdapter(self._db, stmt)
        if isinstance(stmt, Update):
            return UpdateAdapter(self._db, stmt)
        if isinstance(stmt, Delete):
            return DeleteAdapter(self._db, stmt)
        return stmt

    def __getattr__(self, item: str):
        attr = getattr(self._stmt, item)
        if callable(attr):

            def _wrapped(*args: Any, **kwargs: Any):
                return self._wrap(attr(*args, **kwargs))

            return _wrapped
        return attr

    def __clause_element__(self):
        return self._stmt

    @property
    def statement(self):
        """Return the wrapped SQLAlchemy statement."""
        return self._stmt

    async def execute(self, params: Any = None, **kwargs: Any):
        """Execute the wrapped statement in a managed session."""
        execute_params = params if params is not None else kwargs
        async with self._db.session() as session:
            return await session.execute(self._stmt, execute_params)

    async def all(self, params: Any = None, **kwargs: Any):
        """Execute and return all rows, adapting select rows when needed."""
        result = await self.execute(params, **kwargs)
        if isinstance(self._stmt, Select):
            if _is_single_model_select(self._stmt):
                return result.scalars().all()
            return [RowAdapter(row) for row in result.all()]
        return result.all()

    async def first(self, params: Any = None, **kwargs: Any):
        """Execute and return the first row, adapting select rows when needed."""
        result = await self.execute(params, **kwargs)
        if isinstance(self._stmt, Select):
            if _is_single_model_select(self._stmt):
                return result.scalars().first()
            row = result.first()
            return RowAdapter(row) if row is not None else None
        return result.first()

    async def scalar(self, params: Any = None, **kwargs: Any):
        """Execute and return the first scalar value."""
        result = await self.execute(params, **kwargs)
        return result.scalar()

    async def status(self, params: Any = None, **kwargs: Any):
        """Execute and return the affected row count when available."""
        result = await self.execute(params, **kwargs)
        return getattr(result, "rowcount", None)

    async def iterate(self, params: Any = None, **kwargs: Any):
        """Stream rows from the wrapped statement."""
        execute_params = params if params is not None else kwargs
        async with self._db.session() as session:
            async_result = await session.stream(self._stmt, execute_params)
            if isinstance(self._stmt, Select) and _is_single_model_select(self._stmt):
                async for row in async_result.scalars():
                    yield row
            else:
                async for row in async_result:
                    yield RowAdapter(row)


class SelectAdapter(StatementAdapter):
    pass


class InsertAdapter(StatementAdapter):
    async def all(self, rows: Any = None, **kwargs: Any):
        """Execute an insert and return rows only for returning statements."""
        payload = rows if rows is not None else kwargs
        async with self._db.session() as session:
            result = await session.execute(self._stmt, payload)
            return result.all() if getattr(result, "returns_rows", False) else []


class UpdateAdapter(StatementAdapter):
    pass


class DeleteAdapter(StatementAdapter):
    pass


class FuncProxy:
    def __getattr__(self, item: str):
        attr = getattr(sa_func, item)

        def _call(*args: Any, **kwargs: Any):
            return attr(*args, **kwargs)

        return _call
