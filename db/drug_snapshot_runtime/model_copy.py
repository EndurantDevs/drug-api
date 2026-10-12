# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native model acquisition shared by ordinary and archive producers."""

import asyncio
import math
import re
from tempfile import TemporaryFile


class NativeModelCopyError(ValueError):
    """A model batch or its caller-owned native transaction is incompatible."""


async def copy_record_batch(session, model_type, *, schema_name, table_name, columns, records, timeout=300):
    """Append a fixed-size model batch without INSERT fallback or row hooks."""
    if (
        tuple(columns) != tuple(column.name for column in model_type.__table__.columns)
        or not isinstance(records, (list, tuple))
        or not 0 < len(records) <= 5000
        or type(timeout) not in (int, float)
        or not math.isfinite(timeout)
        or not 0 < timeout <= 300
        or any(
            not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)
            for name in (schema_name, table_name)
        )
    ):
        raise NativeModelCopyError("native model acquisition COPY scope differs")
    driver = await transaction_driver(session, ("copy_records_to_table",))
    try:
        async with asyncio.timeout(timeout):
            copied = await driver.copy_records_to_table(
                table_name, schema_name=schema_name, columns=tuple(columns), records=records, timeout=timeout
            )
    except asyncio.CancelledError, TimeoutError:
        driver.terminate()
        raise
    if copied != f"COPY {len(records)}":
        raise NativeModelCopyError("native model acquisition COPY count differs")
    return len(records)


async def transaction_driver(session, methods):
    """Require the actual caller-owned transaction and every native protocol method."""
    if callable(getattr(session, "is_in_transaction", None)):
        driver = session
    else:
        if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
            raise NativeModelCopyError("native model acquisition COPY is unavailable")
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
    if (
        not callable(getattr(driver, "is_in_transaction", None))
        or not driver.is_in_transaction()
        or any(not callable(getattr(driver, name, None)) for name in methods)
    ):
        raise NativeModelCopyError("native model acquisition COPY is unavailable")
    return driver


async def copy_projection(session, query, *args, schema_name, table_name, columns, max_bytes, timeout):
    """Drain and restore native wire bytes sequentially within the owning transaction."""
    if (
        (type(max_bytes) is not int or not 0 <= max_bytes < 2**63)
        or (type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 86400)
        or (not isinstance(query, str) or not query.startswith("SELECT "))
        or (not isinstance(columns, (list, tuple)) or not 1 <= len(columns) <= 1600)
        or any(
            not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)
            for name in (schema_name, table_name, *columns)
        )
        or len(set(columns)) != len(columns)
    ):
        raise NativeModelCopyError("native model projection COPY bounds differ")
    driver = await transaction_driver(session, ("copy_from_query", "copy_to_table"))
    return await restore_projection(driver, query, schema_name, table_name, columns, max_bytes, timeout, *args)


async def capture_projection(driver, query, spool, max_bytes, deadline, *query_args, copy_format="binary"):
    """Drain native COPY OUT with a fixed spool cap before restoring any payload."""
    if copy_format not in {"binary", "text"}:
        raise NativeModelCopyError("native model projection COPY format differs")
    accounting_by_field = {"copied_bytes": 0, "error": None}

    async def consume(chunk):
        """Drain after a cap or spool failure without retaining additional bytes."""
        if accounting_by_field["error"] is not None:
            return
        if accounting_by_field["copied_bytes"] + len(chunk) > max_bytes:
            accounting_by_field["error"] = NativeModelCopyError("native model projection COPY byte cap exceeded")
            return
        try:
            spool.write(chunk)
        except Exception as error:
            accounting_by_field["error"] = error
            return
        accounting_by_field["copied_bytes"] += len(chunk)

    status = await driver.copy_from_query(
        query, *query_args, output=consume, format=copy_format, timeout=deadline - asyncio.get_running_loop().time()
    )
    if accounting_by_field["error"] is not None:
        raise accounting_by_field["error"]
    return status, accounting_by_field["copied_bytes"]


async def restore_projection(driver, query, schema_name, table_name, columns, max_bytes, timeout, *query_args):
    """Keep sequential COPY OUT/IN inside one native deadline and caller-owned transaction."""
    with TemporaryFile(mode="w+b") as spool:
        try:
            async with asyncio.timeout(timeout) as deadline:
                captured, copied_bytes = await capture_projection(
                    driver, query, spool, max_bytes, deadline.when(), *query_args
                )
                spool.seek(0)
                restored = await driver.copy_to_table(
                    table_name,
                    schema_name=schema_name,
                    columns=tuple(columns),
                    source=spool,
                    format="binary",
                    timeout=deadline.when() - asyncio.get_running_loop().time(),
                )
                if not isinstance(captured, str) or not re.fullmatch(r"COPY [0-9]+", captured) or captured != restored:
                    raise NativeModelCopyError("native model projection COPY count differs")
        except asyncio.CancelledError, TimeoutError:
            driver.terminate()
            raise
    return copied_bytes
