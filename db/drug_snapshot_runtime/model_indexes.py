# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compile trusted local model indexes for isolated heaps and archive witnesses."""

import hashlib
import re

from sqlalchemy import ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import AddConstraint, CreateIndex


class NativeModelIndexError(ValueError):
    """A local model declaration is outside the reviewed index grammar."""


def quoted(value):
    """Quote one trusted model identifier, never an SQL fragment."""
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
        raise NativeModelIndexError("reference family identifier is invalid")
    return f'"{value}"'


def index_name(table_name, base_name):
    """Keep the established bounded native index naming convention."""
    if len(base_name) <= 63:
        return base_name
    digest = hashlib.sha1(base_name.encode("utf-8")).hexdigest()[:10]
    prefix = table_name[: 63 - len(digest) - 1].rstrip("_")
    return f"{prefix}_{digest}"


def additional_index_sql(
    schema_name, model_type, index_spec, *, table_name=None, index_schema_name=None, resolved_opclasses=None
):
    """Compile the same reviewed local options, names and native SQL."""
    allowed_keys = {"index_elements", "name", "using", "unique", "include", "where", "staging_name"}
    if set(index_spec) - allowed_keys:
        raise NativeModelIndexError("reference family model index is unsupported")
    staging_name = index_spec.get("staging_name")
    if staging_name is not None and (
        not isinstance(staging_name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", staging_name) is None
    ):
        raise NativeModelIndexError("reference family model staging index name is invalid")
    elements = index_spec.get("index_elements")
    if (
        not isinstance(elements, (tuple, list))
        or not elements
        or not all(is_reviewed_index_element(element) for element in elements)
    ):
        raise NativeModelIndexError("reference family model index is invalid")
    suffix = index_spec.get("name", "_".join(elements))
    if not isinstance(suffix, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", suffix) is None:
        raise NativeModelIndexError("reference family model index is invalid")
    name = index_name(
        model_type.__tablename__, f"{index_schema_name or schema_name}_{model_type.__tablename__}_idx_{suffix}"
    )
    method = index_spec.get("using")
    if method is not None and method not in {"btree", "gin", "gist", "hash", "brin", "spgist"}:
        raise NativeModelIndexError("reference family model index method is invalid")
    is_unique = index_spec.get("unique", False)
    if type(is_unique) is not bool:
        raise NativeModelIndexError("reference family model index uniqueness is invalid")
    included = index_spec.get("include", ())
    if not isinstance(included, (tuple, list)) or not all(
        isinstance(column, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) is not None for column in included
    ):
        raise NativeModelIndexError("reference family model index include is invalid")
    using = f" USING {method}" if method else ""
    unique = "UNIQUE " if is_unique else ""
    include = f" INCLUDE ({', '.join(included)})" if included else ""
    where = reviewed_index_where(index_spec.get("where"))
    compiled_elements = [
        witness_index_element(model_type, method or "btree", element, resolved_opclasses) for element in elements
    ]
    return (
        f"CREATE {unique}INDEX {quoted(name)} ON {quoted(schema_name)}."
        f"{quoted(table_name or model_type.__tablename__)}{using} ({', '.join(compiled_elements)}){include}{where}"
    )


def reviewed_index_where(where):
    """Admit only the existing closed model predicate declarations."""
    if where not in {
        None,
        "estimated_gross_margin IS NOT NULL",
        "rx_code_system = 'HP_RX_CODE'",
        "geography_scope = 'zip5'",
        "type='practice'",
        "type='practice' AND phone_number IS NOT NULL AND phone_number <> ''",
    }:
        raise NativeModelIndexError("reference family model index predicate is unsupported")
    return "" if where is None else f" WHERE {where}"


def model_index_input(model, method, element):
    """Resolve the genuine model column and its reviewed operator class."""
    parts = element.split()
    opclass = parts[-1] if parts[-1].endswith("_ops") else None
    column = parts[0] if parts[0] in model.__table__.columns else None
    return method, opclass, column


def witness_index_element(model, method, element, resolved_opclasses):
    """Use the witnessed native operator class without changing model ordering."""
    if resolved_opclasses is None:
        return element
    key = model_index_input(model, method, element)
    expression = element.rsplit(" ", 1)[0] if key[1] is not None else element
    ordering = ""
    if expression.endswith((" DESC", " ASC")):
        expression, ordering = expression.rsplit(" ", 1)
        ordering = " " + ordering
    opclass = ".".join(quoted(part) for part in resolved_opclasses[key])
    return f"{expression} {opclass}{ordering}"


def is_reviewed_index_element(value):
    """Refuse expressions outside the established local model grammar."""
    if not isinstance(value, str):
        return False
    tokens = value.split()
    if 0 < len(tokens) <= 2 and all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token) is not None for token in tokens):
        return True
    return value in {
        "lower(synonym)",
        "lower(provider_type)",
        "lower(provider_name)",
        "lower(service_description)",
        "lower(reported_code)",
        "lower(display_name)",
        "lower(rx_name)",
        "lower(generic_name)",
        "lower(brand_name)",
        "lower(COALESCE(rx_name, '')) gin_trgm_ops",
        "lower(COALESCE(generic_name, '')) gin_trgm_ops",
        "lower(COALESCE(brand_name, '')) gin_trgm_ops",
        "lower(COALESCE(rx_code, '')) gin_trgm_ops",
        "lower(short_description)",
        "upper(from_system)",
        "upper(from_code)",
        "upper(to_system)",
        "upper(to_code)",
        "LEFT(postal_code, 5)",
        "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
    }


def declared_index_statements(
    model, table, schema_name, indexes, *, index_schema_name=None, native_indexes=True, resolved_opclasses=None
):
    """Compile indexes only from genuine local model declarations."""
    native = (
        tuple(CreateIndex(index) for index in sorted(table.indexes, key=lambda item: item.name))
        if native_indexes
        else ()
    )
    return native + tuple(
        text(
            additional_index_sql(
                schema_name,
                model,
                index,
                table_name=table.name,
                index_schema_name=index_schema_name,
                resolved_opclasses=resolved_opclasses,
            )
        )
        for index in indexes
    )


def defer_table_constraints(table):
    """Keep columns intact while delaying native constraints until family completion."""
    for column in table.columns:
        for constraint in tuple(column.constraints):
            column.constraints.remove(constraint)
            table.append_constraint(constraint)
    for constraint in table.constraints:
        constraint.ddl_if(callable_=lambda *_args, **_kwargs: False)


async def create_table_constraints(session, table, *, backing_indexes=True):
    """Finish model keys or checks; never install per-record relationship constraints."""
    if not backing_indexes:
        for column in table.columns:
            for constraint in tuple(column.constraints):
                column.constraints.remove(constraint)
                table.append_constraint(constraint)
    constraints = (
        constraint
        for constraint in table.constraints
        if not isinstance(constraint, ForeignKeyConstraint)
        and isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)) == backing_indexes
        and (not isinstance(constraint, PrimaryKeyConstraint) or constraint.columns)
    )
    for constraint in sorted(
        constraints, key=lambda item: (type(item).__name__, str(item.name or ""), tuple(item.columns.keys()))
    ):
        statement = AddConstraint(constraint)
        if callable(getattr(session, "is_in_transaction", None)):
            if not session.is_in_transaction():
                raise NativeModelIndexError("model DDL requires a caller transaction")
            await session.execute(str(statement.compile(dialect=postgresql.dialect())))
        else:
            await session.execute(statement)
