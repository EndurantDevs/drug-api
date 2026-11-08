"""The standalone model copier needs no application startup."""

import asyncio
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.drug_snapshot_runtime import model_copy
from db.drug_snapshot_runtime.ndc import Product


def test_model_runtime_is_application_independent():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from db.drug_snapshot_runtime import model_copy, model_indexes, ndc_publication; "
            "assert 'sanic' not in sys.modules; assert 'db.connection' not in sys.modules",
        ],
        check=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "cap", "count", "cancel"])
async def test_shared_projection_drains_before_restore_and_preserves_failure(failure):
    """Exercise the shared native protocol seam, not a database or publication claim."""
    events = []

    async def copy_out(query, *, output, **_options):
        events.append("out")
        if failure == "cancel":
            raise asyncio.CancelledError
        await output(b"first")
        await output(b"second")
        events.append("drained")
        return "COPY 1"

    async def copy_in(table_name, *, source, **_options):
        assert events == ["out", "drained"] and source.read() == b"firstsecond"
        events.append("in")
        return "COPY 2" if failure == "count" else "COPY 1"

    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        copy_from_query=copy_out,
        copy_to_table=copy_in,
        terminate=lambda: events.append("terminated"),
    )
    copy_options_by_field = {
        "schema_name": "synthetic",
        "table_name": "product_candidate",
        "columns": ("product_id",),
        "max_bytes": 1 if failure == "cap" else 1024,
        "timeout": 10,
    }
    if failure is None:
        assert (
            await model_copy.copy_projection(
                driver, "SELECT product_id FROM synthetic.product", **copy_options_by_field
            )
            == 11
        )
        assert events == ["out", "drained", "in"]
    elif failure == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await model_copy.copy_projection(
                driver, "SELECT product_id FROM synthetic.product", **copy_options_by_field
            )
        assert events == ["out", "terminated"]
    else:
        with pytest.raises(model_copy.NativeModelCopyError, match="byte cap|count differs"):
            await model_copy.copy_projection(
                driver, "SELECT product_id FROM synthetic.product", **copy_options_by_field
            )
        assert events == (["out", "drained"] if failure == "cap" else ["out", "drained", "in"])


@pytest.mark.asyncio
async def test_shared_copy_refuses_an_unowned_transaction():
    driver = SimpleNamespace(is_in_transaction=lambda: False, copy_from_query=AsyncMock(), copy_to_table=AsyncMock())
    with pytest.raises(model_copy.NativeModelCopyError, match="unavailable"):
        await model_copy.copy_projection(
            driver,
            "SELECT product_id FROM synthetic.product",
            schema_name="synthetic",
            table_name="product_candidate",
            columns=("product_id",),
            max_bytes=1024,
            timeout=10,
        )
    driver.copy_from_query.assert_not_called()
    driver.copy_to_table.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "count", "cancel", "timeout"])
async def test_acquisition_uses_the_real_model_columns_and_owning_native_driver(failure):
    columns = tuple(Product.__table__.columns.keys())
    import_records = [tuple(None for _column in columns)]
    events = []

    async def copy_records(table_name, **arguments):
        assert table_name == "product_candidate"
        assert arguments == {"schema_name": "synthetic", "columns": columns, "records": import_records, "timeout": 10}
        events.append("copy")
        if failure == "cancel":
            raise asyncio.CancelledError
        if failure == "timeout":
            raise TimeoutError
        return "COPY 0" if failure == "count" else "COPY 1"

    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        copy_records_to_table=copy_records,
        terminate=lambda: events.append("terminated"),
    )
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(in_transaction=lambda: True, connection=AsyncMock(return_value=connection))
    copy_options_by_field = {
        "schema_name": "synthetic",
        "table_name": "product_candidate",
        "columns": columns,
        "records": import_records,
        "timeout": 10,
    }
    if failure is None:
        assert await model_copy.copy_record_batch(session, Product, **copy_options_by_field) == 1
    else:
        error = {"count": model_copy.NativeModelCopyError, "cancel": asyncio.CancelledError, "timeout": TimeoutError}[
            failure
        ]
        with pytest.raises(error):
            await model_copy.copy_record_batch(session, Product, **copy_options_by_field)
    assert events == (["copy", "terminated"] if failure in {"cancel", "timeout"} else ["copy"])
    session.connection.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"columns": ("product_id",)},
        {"records": []},
        {"records": [()] * 5001},
        {"timeout": True},
        {"timeout": float("inf")},
        {"timeout": 301},
        {"schema_name": "bad;sql"},
        {"table_name": "bad.name"},
    ],
)
async def test_acquisition_refuses_unbounded_or_foreign_scope_before_a_connection(change):
    session = SimpleNamespace(in_transaction=lambda: True, connection=AsyncMock())
    copy_options_by_field = {
        "schema_name": "synthetic",
        "table_name": "product_candidate",
        "columns": tuple(Product.__table__.columns.keys()),
        "records": [()],
        **change,
    }
    with pytest.raises(model_copy.NativeModelCopyError, match="scope differs"):
        await model_copy.copy_record_batch(session, Product, **copy_options_by_field)
    session.connection.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"max_bytes": True},
        {"max_bytes": -1},
        {"timeout": 0},
        {"timeout": float("nan")},
        {"query": "INSERT INTO synthetic.product VALUES (1)"},
        {"columns": ("id", "id")},
        {"columns": ("id;sql",)},
        {"columns": []},
    ],
)
async def test_projection_refuses_expanded_bounds_and_sql_before_native_protocol(change):
    copy_options_by_field = {
        "query": "SELECT product_id FROM synthetic.product",
        "schema_name": "synthetic",
        "table_name": "candidate",
        "columns": ("product_id",),
        "max_bytes": 100,
        "timeout": 10,
        **change,
    }
    with pytest.raises(model_copy.NativeModelCopyError, match="bounds differ"):
        await model_copy.copy_projection(object(), **copy_options_by_field)


@pytest.mark.asyncio
async def test_capture_drains_after_a_spool_error_then_raises_that_same_error():
    failure = OSError("synthetic spool failure")
    writes = []
    events = []

    class BrokenSpool:
        def write(self, chunk):
            writes.append(chunk)
            raise failure

    async def copy_out(query, *parameters, **arguments):
        assert query == "SELECT data" and parameters == ("scope",) and arguments["format"] == "text"
        await arguments["output"](b"first")
        await arguments["output"](b"second")
        events.append("drained")
        return "COPY 2"

    driver = SimpleNamespace(copy_from_query=copy_out)
    with pytest.raises(OSError) as caught:
        await model_copy.capture_projection(
            driver,
            "SELECT data",
            BrokenSpool(),
            100,
            asyncio.get_running_loop().time() + 10,
            "scope",
            copy_format="text",
        )
    assert caught.value is failure and writes == [b"first"] and events == ["drained"]


@pytest.mark.asyncio
async def test_copy_requires_available_protocol_and_only_approved_wire_formats():
    with pytest.raises(model_copy.NativeModelCopyError, match="unavailable"):
        await model_copy.transaction_driver(object(), ("copy_to_table",))
    with pytest.raises(model_copy.NativeModelCopyError, match="unavailable"):
        await model_copy.transaction_driver(SimpleNamespace(is_in_transaction=lambda: True), ("copy_to_table",))
    with pytest.raises(model_copy.NativeModelCopyError, match="format differs"):
        await model_copy.capture_projection(object(), "SELECT data", object(), 100, 10, copy_format="csv")
