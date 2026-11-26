from contextlib import asynccontextmanager

import pytest
from sqlalchemy import String, any_, bindparam
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import asyncpg as sa_asyncpg

from api.utils import Package, Product, db
from db.statement_adapters import DEFAULT_STREAM_YIELD_PER, SelectAdapter


def test_name_search_subqueries_compile_with_sqlalchemy_adapter():
    low_subq = (
        db.select([Product.generic_name])
        .select_from(Product)
        .where(Product.brand_name.ilike("aspirin") | Product.generic_name.ilike("aspirin"))
        .group_by(Product.generic_name)
    )

    subq = (
        db.select([Product.product_ndc])
        .select_from(Product)
        .where(
            Product.generic_name.in_(low_subq.statement)
            & (
                (db.func.lower(Product.generic_name) == db.func.lower(Product.brand_name))
                | Product.brand_name.is_(None)
            )
        )
    )

    query = (
        db.select(
            [
                Product.generic_name,
                Product.brand_name,
                Product.dosage_form,
                Product.short_dosage_form,
                Package.package_ndc,
                Package.description,
                Package.size,
                Package.size_extra,
                Package.packages_number,
                Package.package_format,
            ]
        )
        .select_from(Product.__table__.join(Package.__table__, Product.product_ndc == Package.product_ndc))
        .where(Product.product_ndc.in_(subq.statement))
    )

    compiled = str(query.statement.compile(dialect=postgresql.dialect()))

    assert "SELECT" in compiled
    assert "JOIN" in compiled
    assert "product_ndc IN" in compiled


def test_package_ndc_any_array_compiles_stable_sql_across_list_sizes():
    dialect = sa_asyncpg.dialect()
    single_param = bindparam("product_ndcs", value=["63323-476"], type_=ARRAY(String))
    multi_param = bindparam(
        "product_ndcs",
        value=["63323-476", "51662-1407", "10019-929"],
        type_=ARRAY(String),
    )

    single_query = Package.query.where(Package.product_ndc == any_(single_param))
    multi_query = Package.query.where(Package.product_ndc == any_(multi_param))

    single_sql = str(single_query.statement.compile(dialect=dialect))
    multi_sql = str(multi_query.statement.compile(dialect=dialect))

    assert single_sql == multi_sql
    assert "= ANY ($1::VARCHAR[])" in single_sql


@pytest.mark.asyncio
async def test_statement_adapter_iterate_applies_default_yield_per():
    captured_stream_options = []

    class _FakeScalarStream:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    class _FakeAsyncResult:
        def scalars(self):
            return _FakeScalarStream()

    class _FakeSession:
        async def stream(self, stmt, _params):
            captured_stream_options.append(stmt.get_execution_options())
            return _FakeAsyncResult()

    class _FakeDatabase:
        @asynccontextmanager
        async def session(self):
            yield _FakeSession()

    adapter = SelectAdapter(_FakeDatabase(), Package.query.statement)
    streamed_packages = [package_row async for package_row in adapter.iterate()]

    assert streamed_packages == []
    assert captured_stream_options == [{"yield_per": DEFAULT_STREAM_YIELD_PER}]
