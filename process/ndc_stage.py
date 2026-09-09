"""Attempt-owned NDC staging and checked paired-table publication evidence."""

import datetime
import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from itertools import batched

from sqlalchemy import MetaData, select, text
from sqlalchemy.dialects.postgresql import insert

from db.models import Package, Product


@dataclass
class NdcAttempt:
    """One coordinator's exclusive stages; counters advance only after paired saves."""

    run_id: str
    suffix: str
    schema: str
    tables: dict
    owner_comment: str
    table_oids: dict = field(default_factory=dict)
    incumbent_oids: dict = field(default_factory=dict)
    counts: dict = field(default_factory=lambda: {
        "source_products": 0, "source_packages": 0,
        "product": 0, "package": 0, "batches": 0,
    })


def new_ndc_attempt(run_id: str, schema: str) -> NdcAttempt:
    """Allocate an exclusive name without adopting or deleting existing stages."""
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema):
        raise ValueError("unsupported NDC schema identifier")
    suffix = "n" + uuid.uuid4().hex
    metadata = MetaData()
    tables_by_name = {model.__tablename__: model.__table__.to_metadata(
        metadata, schema=schema, name=f"{model.__tablename__}_{suffix}",
    ) for model in (Product, Package)}
    owner_comment = json.dumps({"format": "ndc-stage-v1", "run_id": run_id, "attempt_id": suffix}, sort_keys=True)
    return NdcAttempt(run_id, suffix, schema, tables_by_name, owner_comment)


async def create_ndc_stages(database, attempt: NdcAttempt, *, standalone: bool) -> None:
    """Claim the native run and create both private tables in one transaction."""
    async with database.transaction() as session:
        await database.status("SET LOCAL lock_timeout = '5s'")
        await database.status("SET LOCAL statement_timeout = '120s'")
        if standalone:
            await database.status(text(f"""
                INSERT INTO {attempt.schema}.import_run
                    (run_id, engine, importer, family, status, created_at, params)
                VALUES (:run_id, 'drug-api', 'ndc', 'drug', 'queued', CURRENT_TIMESTAMP, '{{}}'::jsonb)
            """), run_id=attempt.run_id)
        changed = await database.status(text(f"""
            UPDATE {attempt.schema}.import_run
               SET metrics=COALESCE(metrics, '{{}}'::jsonb) ||
                   jsonb_build_object('ndc_attempt_id', CAST(:attempt_id AS text)),
                   status='running', started_at=CURRENT_TIMESTAMP,
                   heartbeat_at=CURRENT_TIMESTAMP, finished_at=NULL, error=NULL, phase_detail='ndc acquiring source'
             WHERE run_id=:run_id AND importer='ndc' AND NOT (COALESCE(metrics, '{{}}'::jsonb) ? 'ndc_attempt_id')
               AND status IN ('queued', 'starting', 'running')
        """), run_id=attempt.run_id, attempt_id=attempt.suffix)
        if changed != 1:
            raise RuntimeError("NDC run is no longer claimable")
        attempt.incumbent_oids = await ndc_table_oids(database, attempt.schema)
        connection = await session.connection()
        for table in attempt.tables.values():
            await connection.run_sync(table.create)
            await _comment_ndc_table(database, attempt.schema, table.name, attempt.owner_comment)
        attempt.table_oids = await ndc_table_oids(database, attempt.schema, attempt.suffix)


async def ndc_table_oids(database, schema: str, suffix: str = "") -> dict:
    """Read the identities of both named tables, preserving explicit absence."""
    tail = "_" + suffix if suffix else ""
    return {name: await database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{schema}.{name}{tail}")
            for name in ("product", "package")}


async def lock_ndc_stages(database, attempt: NdcAttempt, *, exclusive: bool = False) -> None:
    """Lock both stages in stable order and reject foreign table identities."""
    await database.status("SET LOCAL lock_timeout = '5s'")
    await database.status("SET LOCAL statement_timeout = '120s'")
    mode = "ACCESS EXCLUSIVE" if exclusive else "ROW EXCLUSIVE"
    names = ", ".join(f"{attempt.schema}.{name}_{attempt.suffix}" for name in ("product", "package"))
    await database.status(f"LOCK TABLE {names} IN {mode} MODE")
    if await ndc_table_oids(database, attempt.schema, attempt.suffix) != attempt.table_oids:
        raise RuntimeError("NDC staging identity changed")
    for table_oid in attempt.table_oids.values():
        comment = await database.scalar(text("SELECT obj_description(:oid, 'pg_class')"), oid=table_oid)
        if comment != attempt.owner_comment:
            raise RuntimeError("NDC stage ownership changed")


async def save_ndc_batch(database, attempt: NdcAttempt, products: list, packages: list) -> None:
    """Persist a complete paired batch, allowing only identical duplicate keys."""
    async with database.transaction() as session:
        await lock_ndc_stages(database, attempt)
        await _check_ndc_run_owner(database, attempt)
        product_count = await _insert_verified_rows(session, attempt.tables["product"], products, "product_id")
        package_count = await _insert_verified_rows(session, attempt.tables["package"], packages, "package_ndc")
    attempt.counts["product"] += product_count
    attempt.counts["package"] += package_count
    attempt.counts["source_products"] += len(products)
    attempt.counts["source_packages"] += len(packages)
    attempt.counts["batches"] += 1


def _unique_ndc_rows(rows: list, primary_key: str) -> dict:
    rows_by_key = {}
    for row in rows:
        identity = row.get(primary_key)
        if not isinstance(identity, str) or not identity.strip():
            raise ValueError("NDC row identity is missing")
        previous = rows_by_key.setdefault(identity, row)
        if previous != row:
            raise ValueError("conflicting NDC records share an identity")
    return rows_by_key


async def _insert_verified_rows(session, table, rows: list, primary_key: str) -> int:
    rows_by_key = _unique_ndc_rows(rows, primary_key)
    if not rows_by_key:
        return 0
    statement = insert(table).on_conflict_do_nothing(index_elements=[primary_key]).returning(table.c[primary_key])
    inserted_count = 0
    for row_batch in batched(rows_by_key.values(), 500):
        inserted = await session.execute(statement, list(row_batch))
        inserted_count += len(inserted.all())
        expected_by_key = {row[primary_key]: row for row in row_batch}
        persisted = await session.execute(select(table).where(table.c[primary_key].in_(expected_by_key)))
        persisted_by_key = {row[primary_key]: dict(row) for row in persisted.mappings()}
        if persisted_by_key != expected_by_key:
            raise RuntimeError("persisted NDC batch differs from acquired records")
    return inserted_count


async def _check_ndc_run_owner(database, attempt: NdcAttempt) -> None:
    run_id = await database.scalar(text(f"""
        SELECT run_id FROM {attempt.schema}.import_run
         WHERE run_id=:run_id AND importer='ndc' AND metrics->>'ndc_attempt_id'=:attempt_id AND status='running'
         FOR SHARE
    """), run_id=attempt.run_id, attempt_id=attempt.suffix)
    if run_id != attempt.run_id:
        raise RuntimeError("NDC run ownership or status changed")


async def audit_ndc_tables(database, session, attempt: NdcAttempt, acquisition: dict) -> dict:
    """Hash exact locked native CSV bytes and require the full persisted census."""
    if sum(part["records"] for part in acquisition["partitions"]) != attempt.counts["source_products"]:
        raise RuntimeError("NDC acquired and persisted record census differs")
    connection = await session.connection()
    raw_connection = await connection.get_raw_connection()
    tables_by_name = {}
    for name, table in attempt.tables.items():
        tables_by_name[name] = await _audit_locked_ndc_table(database, raw_connection.driver_connection, attempt, name, table)
    return {
        "format": "ndc-publication-v1", "run_id": attempt.run_id,
        "attempt_id": attempt.suffix, "complete": acquisition["complete"],
        "acquisition": acquisition, "counts": dict(attempt.counts), "tables": tables_by_name,
    }


async def _audit_locked_ndc_table(database, driver, attempt: NdcAttempt, name: str, table) -> dict:
    primary_key = "product_id" if name == "product" else "package_ndc"
    table_name = f"{attempt.schema}.{table.name}"
    row_count = await database.scalar(f"SELECT count(*) FROM {table_name}")
    if row_count != attempt.counts[name]:
        raise RuntimeError("NDC stage census differs from acknowledged inserts")
    csv_digest = _NdcCsvDigest()
    columns = ", ".join('"' + column.name + '"' for column in table.columns)
    completion = await driver.copy_from_query(
        f'SELECT {columns} FROM {table_name} ORDER BY "{primary_key}" COLLATE "C"',
        output=csv_digest.write, format="csv", timeout=120,
    )
    if completion != f"COPY {row_count}":
        raise RuntimeError("NDC COPY census differs from stage census")
    column_rows = await database.all(text("""
        SELECT attname AS name, format_type(atttypid, atttypmod) AS type, attnotnull AS not_null
        FROM pg_attribute WHERE attrelid=:oid AND attnum>0 AND NOT attisdropped ORDER BY attnum
    """), oid=attempt.table_oids[name])
    return {"oid": attempt.table_oids[name], "row_count": row_count, "size_bytes": csv_digest.size_bytes,
            "sha256": csv_digest.digest.hexdigest(), "encoding": "postgres-copy-csv-v1",
            "columns": [dict(row) for row in column_rows]}


async def check_ndc_incumbents(database, attempt: NdcAttempt) -> None:
    """Use a non-waiting publication fence, then lock and compare the live pair."""
    acquired = await database.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:scope, 0))"),
                                     scope=f"{attempt.schema}:ndc-publication")
    if not acquired:
        raise RuntimeError("another NDC publication is active")
    for name, table_oid in attempt.incumbent_oids.items():
        if table_oid is not None:
            await database.status(f"LOCK TABLE {attempt.schema}.{name} IN ACCESS EXCLUSIVE MODE NOWAIT")
    if await ndc_table_oids(database, attempt.schema) != attempt.incumbent_oids:
        raise RuntimeError("NDC incumbent changed during acquisition")


async def finish_ndc_publication(database, attempt: NdcAttempt, receipt: dict, *, published: bool) -> dict:
    """Record success and both physical bindings in the caller's swap transaction."""
    expected_suffix = "" if published else attempt.suffix
    if await ndc_table_oids(database, attempt.schema, expected_suffix) != attempt.table_oids:
        raise RuntimeError("NDC final table identity differs from audited stages")
    phase = "ndc import published" if published else "ndc sample staged"
    receipt_dict = {**receipt, "published": published, "completed_at": datetime.datetime.now(datetime.UTC).isoformat()}
    metrics_dict = {"ndc_attempt_id": attempt.suffix,
                    "ndc_publication" if published else "ndc_sample": receipt_dict,
                    "source_product_count": attempt.counts["source_products"],
                    "imported_product_count": attempt.counts["product"]}
    changed = await database.status(text(f"""
        UPDATE {attempt.schema}.import_run
           SET status='succeeded', phase_detail=:phase, heartbeat_at=CURRENT_TIMESTAMP,
               finished_at=CURRENT_TIMESTAMP, metrics=CAST(:metrics AS jsonb), error=NULL,
               progress=CAST(:progress AS jsonb)
         WHERE run_id=:run_id AND importer='ndc' AND metrics->>'ndc_attempt_id'=:attempt_id AND status='running'
    """), run_id=attempt.run_id, attempt_id=attempt.suffix, phase=phase, metrics=json.dumps(metrics_dict),
        progress=json.dumps({"unit": "records", "done": attempt.counts["source_products"],
                             "total": attempt.counts["source_products"], "pct": 100, "message": phase}))
    if changed != 1:
        raise RuntimeError("NDC terminal success lost its run ownership fence")
    comment = json.dumps(receipt_dict, sort_keys=True)
    for name in attempt.tables:
        table_name = name if published else f"{name}_{attempt.suffix}"
        await _comment_ndc_table(database, attempt.schema, table_name, comment)
    return receipt_dict


async def fail_ndc_attempt(database, attempt: NdcAttempt) -> int:
    """Fail only this still-running owner; committed success can never be relabeled."""
    return await database.status(text(f"""
        UPDATE {attempt.schema}.import_run
           SET status='failed', phase_detail='ndc import failed', finished_at=CURRENT_TIMESTAMP,
               error='{{"code":"ndc_import_failed"}}'::jsonb
         WHERE run_id=:run_id AND importer='ndc' AND metrics->>'ndc_attempt_id'=:attempt_id AND status='running'
    """), run_id=attempt.run_id, attempt_id=attempt.suffix)


async def _comment_ndc_table(database, schema: str, table_name: str, comment: str) -> None:
    quoted_comment = "'" + comment.replace("'", "''") + "'"
    await database.status(f"COMMENT ON TABLE {schema}.{table_name} IS {quoted_comment}")


@dataclass
class _NdcCsvDigest:
    digest: object = field(default_factory=hashlib.sha256)
    size_bytes: int = 0

    async def write(self, chunk) -> None:
        """Hash each awaited native COPY callback without retaining its buffer."""
        self.digest.update(chunk)
        self.size_bytes += len(chunk)
