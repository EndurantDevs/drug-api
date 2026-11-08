"""Compile real local declarations and refuse unsafe index or constraint inputs."""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import CheckConstraint, Column, ForeignKey, Index, Integer, MetaData, Table, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import AddConstraint, CreateTable

from db.drug_snapshot_runtime import model_indexes as indexes
from db.drug_snapshot_runtime.ndc import Package, Product


def test_genuine_ndc_indexes_keep_nonunique_parent_and_separate_native_keys():
    for model in (Product, Package):
        table = model.__table__.to_metadata(MetaData(), schema="candidate", name=model.__tablename__ + "_copy")
        statements = indexes.declared_index_statements(
            model, table, "candidate", model.__my_additional_indexes__, index_schema_name="published"
        )
        assert len(statements) == len(model.__my_additional_indexes__)
        for statement, declaration in zip(statements, model.__my_additional_indexes__, strict=True):
            sql = str(statement)
            assert sql.startswith(f'CREATE INDEX "published_{model.__tablename__}_idx_')
            assert f'ON "candidate"."{table.name}" USING gin' in sql
            assert "UNIQUE" not in sql
            assert declaration["index_elements"][0] in sql
        assert tuple(column.name for column in table.primary_key) == (
            "product_id" if model is Product else "package_ndc",
        )


def test_model_index_names_and_witnessed_operator_classes_are_stable():
    base_name = "long_" * 20
    name = indexes.index_name("candidate_product_" * 5, base_name)
    assert len(name) <= 63 and name.endswith("_" + hashlib.sha1(base_name.encode()).hexdigest()[:10])
    assert indexes.index_name("product", "short") == "short"
    index_by_field = {
        "index_elements": ["product_id DESC"],
        "name": "ordered",
        "using": "btree",
        "unique": True,
        "include": ["brand_name"],
        "where": "rx_code_system = 'HP_RX_CODE'",
    }
    statement = indexes.additional_index_sql(
        "candidate",
        Product,
        index_by_field,
        resolved_opclasses={("btree", None, "product_id"): ("pg_catalog", "text_ops")},
    )
    statement = " ".join(statement.split())
    assert 'CREATE UNIQUE INDEX "candidate_product_idx_ordered"' in statement
    assert '(product_id "pg_catalog"."text_ops" DESC) INCLUDE (brand_name)' in statement
    assert statement.endswith(" WHERE rx_code_system = 'HP_RX_CODE'")
    assert (
        indexes.witness_index_element(
            Product,
            "gin",
            "brand_name gin_trgm_ops",
            {("gin", "gin_trgm_ops", "brand_name"): ("extensions", "gin_trgm_ops")},
        )
        == 'brand_name "extensions"."gin_trgm_ops"'
    )


@pytest.mark.parametrize(
    "change",
    [
        {"sql": "SELECT 1"},
        {"staging_name": "bad;name"},
        {"staging_name": 1},
        {"index_elements": []},
        {"index_elements": ["product_id); DROP TABLE product"]},
        {"index_elements": [None]},
        {"name": "bad.name"},
        {"using": "foreign_method"},
        {"unique": 1},
        {"include": "product_id"},
        {"include": ["product_id;SELECT"]},
        {"where": "true OR arbitrary_function()"},
    ],
)
def test_model_index_grammar_refuses_unreviewed_fragments(change):
    with pytest.raises(indexes.NativeModelIndexError):
        indexes.additional_index_sql("candidate", Product, {"index_elements": ["product_id"], **change})


@pytest.mark.parametrize("value", ["bad.name", "bad;name", "bad\nname"])
def test_identifier_quoting_cannot_expand_into_sql(value):
    with pytest.raises(indexes.NativeModelIndexError, match="identifier"):
        indexes.quoted(value)


def test_reviewed_expression_grammar_and_native_index_selection():
    assert indexes.is_reviewed_index_element("lower(COALESCE(rx_name, '')) gin_trgm_ops")
    assert indexes.is_reviewed_index_element("LEFT(postal_code, 5)")
    assert not indexes.is_reviewed_index_element("unreviewed(product_id)")
    table = Table("example", MetaData(), Column("id", Integer), Index("z_last", "id"), Index("a_first", "id"))
    statements = indexes.declared_index_statements(Product, table, "candidate", ())
    assert [statement.element.name for statement in statements] == ["a_first", "z_last"]
    assert indexes.declared_index_statements(Product, table, "candidate", (), native_indexes=False) == ()


def constrained_table():
    metadata = MetaData()
    Table("parent", metadata, Column("id", Integer, primary_key=True))
    return Table(
        "child",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("parent_id", Integer, ForeignKey("parent.id")),
        Column("amount", Integer, CheckConstraint("amount >= 0", name="amount_positive")),
        UniqueConstraint("amount", name="amount_unique"),
    )


@pytest.mark.asyncio
async def test_constraints_are_deferred_then_finished_without_per_record_relationships():
    table = constrained_table()
    indexes.defer_table_constraints(table)
    created = str(CreateTable(table).compile(dialect=postgresql.dialect()))
    assert "CHECK" not in created and "PRIMARY KEY" not in created and "FOREIGN KEY" not in created
    assert "NOT NULL" in created and not table.c.amount.constraints
    session = SimpleNamespace(execute=AsyncMock())
    await indexes.create_table_constraints(session, table)
    keys = [str(call.args[0].compile(dialect=postgresql.dialect())) for call in session.execute.await_args_list]
    assert len(keys) == 2 and any("PRIMARY KEY" in sql for sql in keys) and any("UNIQUE" in sql for sql in keys)
    session.execute.reset_mock()
    await indexes.create_table_constraints(session, table, backing_indexes=False)
    check = session.execute.await_args.args[0]
    assert isinstance(check, AddConstraint) and "CHECK (amount >= 0)" in str(check)
    assert session.execute.await_count == 1
    assert all("FOREIGN KEY" not in sql for sql in keys)


@pytest.mark.asyncio
async def test_native_constraint_completion_requires_the_callers_transaction():
    table = constrained_table()
    driver = SimpleNamespace(is_in_transaction=lambda: False, execute=AsyncMock())
    with pytest.raises(indexes.NativeModelIndexError, match="caller transaction"):
        await indexes.create_table_constraints(driver, table)
    driver.execute.assert_not_awaited()
    driver.is_in_transaction = lambda: True
    await indexes.create_table_constraints(driver, table, backing_indexes=False)
    assert len(driver.execute.await_args_list) == 1
    assert isinstance(driver.execute.await_args.args[0], str)
    assert "CHECK (amount >= 0)" in driver.execute.await_args.args[0]
