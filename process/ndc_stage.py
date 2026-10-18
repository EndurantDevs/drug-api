"""Attempt-owned NDC staging and checked paired-table publication evidence."""

import datetime
import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass, field
from itertools import batched

from sqlalchemy import MetaData, text

from db.drug_snapshot_runtime.model_copy import copy_projection, copy_record_batch
from db.drug_snapshot_runtime.model_indexes import (
    create_table_constraints,
    declared_index_statements,
    defer_table_constraints,
)
from db.drug_snapshot_runtime.ndc import RELATIONSHIPS
from db.drug_snapshot_runtime.ndc_publication import audit_ndc_table
from db.models import Package, Product

# Keep the former str.strip() blank-key policy in the set-based input check.
_NDC_KEY_WHITESPACE = (
    " \t\n\r\v\f\x1c\x1d\x1e\x1f\x85\xa0\u1680"
    "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"
)


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
    counts: dict = field(
        default_factory=lambda: {
            "source_products": 0,
            "source_packages": 0,
            "product": 0,
            "package": 0,
            "batches": 0,
        }
    )
    publication_mode: str = "direct"
    node_id: str | None = None
    input_tables: dict = field(default_factory=dict)
    input_oids: dict = field(default_factory=dict)


def ndc_publication_mode() -> str:
    """Select publication authority only from trusted process configuration."""
    mode = os.getenv("HLTHPRT_NDC_PUBLICATION_MODE", "direct")
    if mode not in {"direct", "handoff"}:
        raise ValueError("HLTHPRT_NDC_PUBLICATION_MODE must be direct or handoff")
    return mode


def new_ndc_attempt(run_id: str, schema: str) -> NdcAttempt:
    """Allocate an exclusive name without adopting or deleting existing stages."""
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema):
        raise ValueError("unsupported NDC schema identifier")
    mode = ndc_publication_mode()
    node_id = os.getenv("HLTHPRT_IMPORT_NODE_ID") if mode == "handoff" else None
    if mode == "handoff" and (
        schema != "rx_data"
        or not node_id
        or node_id != node_id.strip()
        or not isinstance(run_id, str)
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}", run_id) is None
    ):
        raise ValueError("NDC handoff requires rx_data, a configured node and a bounded run identity")
    suffix = "n" + uuid.uuid4().hex
    metadata = MetaData()
    tables_by_name = {
        model.__tablename__: model.__table__.to_metadata(
            metadata,
            schema=schema,
            name=f"{model.__tablename__}_{suffix}",
        )
        for model in (Product, Package)
    }
    owner_comment = json.dumps({"format": "ndc-stage-v1", "run_id": run_id, "attempt_id": suffix}, sort_keys=True)
    input_tables_by_name = {
        name: table.to_metadata(metadata, name=f"{name}_input_{suffix}") for name, table in tables_by_name.items()
    }
    for table in input_tables_by_name.values():
        for column in table.primary_key.columns:
            column.nullable = True
    for table in (*tables_by_name.values(), *input_tables_by_name.values()):
        defer_table_constraints(table)
    return NdcAttempt(
        run_id,
        suffix,
        schema,
        tables_by_name,
        owner_comment,
        publication_mode=mode,
        node_id=node_id,
        input_tables=input_tables_by_name,
    )


async def create_ndc_stages(database, attempt: NdcAttempt, *, standalone: bool) -> None:
    """Claim the native run and create both private tables in one transaction."""
    async with database.transaction() as session:
        await database.status("SET LOCAL lock_timeout = '5s'")
        await database.status("SET LOCAL statement_timeout = '120s'")
        if standalone:
            await database.status(
                text(f"""
                INSERT INTO {attempt.schema}.import_run
                    (run_id, engine, node_id, importer, family, status, created_at, params)
                VALUES (:run_id, 'drug-api', :node_id, 'ndc', 'drug', 'queued', CURRENT_TIMESTAMP, '{{}}'::jsonb)
            """),
                run_id=attempt.run_id,
                node_id=attempt.node_id,
            )
        changed = await database.status(
            text(f"""
            UPDATE {attempt.schema}.import_run
               SET metrics=COALESCE(metrics, '{{}}'::jsonb) ||
                   jsonb_build_object('ndc_attempt_id', CAST(:attempt_id AS text)),
                   status='running', started_at=CURRENT_TIMESTAMP,
                   heartbeat_at=CURRENT_TIMESTAMP, finished_at=NULL, error=NULL, phase_detail='ndc acquiring source'
             WHERE run_id=:run_id AND importer='ndc' AND NOT (COALESCE(metrics, '{{}}'::jsonb) ? 'ndc_attempt_id')
               AND status IN ('queued', 'starting', 'running')
               AND (CAST(:node_id AS text) IS NULL OR (engine='drug-api' AND node_id=:node_id))
        """),
            run_id=attempt.run_id,
            attempt_id=attempt.suffix,
            node_id=attempt.node_id,
        )
        if changed != 1:
            raise RuntimeError("NDC run is no longer claimable")
        attempt.incumbent_oids = await ndc_table_oids(database, attempt.schema)
        connection = await session.connection()
        for table in (*attempt.tables.values(), *attempt.input_tables.values()):
            await connection.run_sync(table.create)
            await _comment_ndc_table(database, attempt.schema, table.name, attempt.owner_comment)
        attempt.table_oids = await ndc_table_oids(database, attempt.schema, attempt.suffix)
        attempt.input_oids = await ndc_input_oids(database, attempt)


async def ndc_table_oids(database, schema: str, suffix: str = "") -> dict:
    """Read the identities of both named tables, preserving explicit absence."""
    tail = "_" + suffix if suffix else ""
    return {
        name: await database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{schema}.{name}{tail}")
        for name in ("product", "package")
    }


async def ndc_input_oids(database, attempt):
    """Resolve only this attempt's model-derived private acquisition pair."""
    return {
        name: await database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{attempt.schema}.{table.name}")
        for name, table in attempt.input_tables.items()
    }


async def lock_ndc_stages(database, attempt: NdcAttempt, *, exclusive: bool = False) -> None:
    """Lock both stages in stable order and reject foreign table identities."""
    await database.status("SET LOCAL lock_timeout = '5s'")
    await database.status("SET LOCAL statement_timeout = '120s'")
    mode = "ACCESS EXCLUSIVE" if exclusive else "ROW EXCLUSIVE"
    owned = (*attempt.tables.values(), *attempt.input_tables.values())
    names = ", ".join(f"{attempt.schema}.{table.name}" for table in sorted(owned, key=lambda table: table.name))
    await database.status(f"LOCK TABLE {names} IN {mode} MODE")
    if await ndc_table_oids(database, attempt.schema, attempt.suffix) != attempt.table_oids:
        raise RuntimeError("NDC staging identity changed")
    if attempt.input_oids and await ndc_input_oids(database, attempt) != attempt.input_oids:
        raise RuntimeError("NDC acquisition identity changed")
    for table_oid in (*attempt.table_oids.values(), *attempt.input_oids.values()):
        comment = await database.scalar(text("SELECT obj_description(:oid, 'pg_class')"), oid=table_oid)
        if comment != attempt.owner_comment:
            raise RuntimeError("NDC stage ownership changed")


async def discard_ndc_stages(database, attempt: NdcAttempt) -> None:
    """Remove only this attempt's unpublished pair after verifying its locked identity."""
    if not attempt.table_oids:
        return
    async with database.transaction():
        current = await ndc_table_oids(database, attempt.schema, attempt.suffix)
        if all(table_oid is None for table_oid in current.values()):
            return
        run_id = await database.scalar(
            text(f"""
            SELECT run_id FROM {attempt.schema}.import_run
             WHERE run_id=:run_id AND importer='ndc' AND metrics->>'ndc_attempt_id'=:attempt_id
               AND (CAST(:node_id AS text) IS NULL OR (engine='drug-api' AND node_id=:node_id))
               AND NOT (COALESCE(metrics, '{{}}'::jsonb) ? 'ndc_handoff')
               AND (status IN ('running','failed','canceling','canceled','cancelled','dead_letter')
                    OR (status='succeeded' AND metrics ? 'ndc_sample'))
             FOR SHARE
        """),
            run_id=attempt.run_id,
            attempt_id=attempt.suffix,
            node_id=attempt.node_id,
        )
        if run_id != attempt.run_id:
            raise RuntimeError("NDC completed handoff is retained for publisher reconciliation")
        await lock_ndc_stages(database, attempt, exclusive=True)
        names = ", ".join(
            f"{attempt.schema}.{table.name}" for table in (*attempt.tables.values(), *attempt.input_tables.values())
        )
        await database.status(f"DROP TABLE {names}")


async def save_ndc_batch(database, attempt: NdcAttempt, products: list, packages: list) -> None:
    """COPY bounded paired acquisition; completion validates duplicate keys set-wise."""
    async with database.transaction() as session:
        await lock_ndc_stages(database, attempt)
        await _check_ndc_run_owner(database, attempt)
        await _copy_ndc_rows(session, attempt.input_tables["product"], Product, products)
        await _copy_ndc_rows(session, attempt.input_tables["package"], Package, packages)
    attempt.counts["source_products"] += len(products)
    attempt.counts["source_packages"] += len(packages)
    attempt.counts["batches"] += 1


async def _copy_ndc_rows(session, table, model, rows: list) -> int:
    """COPY every occurrence with genuine model binding and bounded native batches."""
    if not rows:
        return 0
    dialect = (await session.connection()).dialect
    columns = tuple(model.__table__.columns)
    processors = tuple(column.type.bind_processor(dialect) for column in columns)
    for batch in batched(rows, 5000):
        records = [
            tuple(
                processor(row.get(column.name)) if processor else row.get(column.name)
                for column, processor in zip(columns, processors, strict=True)
            )
            for row in batch
        ]
        await copy_record_batch(
            session,
            model,
            schema_name=table.schema,
            table_name=table.name,
            columns=tuple(column.name for column in columns),
            records=records,
        )
    return len(rows)


async def _validate_ndc_inputs(database, attempt):
    """Check identities and conflicting payloads set-wise in both closed input heaps."""
    for name, model in (("product", Product), ("package", Package)):
        input_table = attempt.input_tables[name]
        key = tuple(model.__table__.primary_key.columns)[0].name
        relation = f'"{attempt.schema}"."{input_table.name}"'
        await database.status(f'CREATE INDEX "{input_table.name}_key" ON {relation}("{key}")')
        if await database.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {relation} acquired "
                f'GROUP BY "{key}" HAVING "{key}" IS NULL OR btrim("{key}", :blank_key_chars)=\'\' '
                f"OR count(DISTINCT to_jsonb(acquired))>1)"
            ),
            blank_key_chars=_NDC_KEY_WHITESPACE,
        ):
            raise RuntimeError("NDC input has a missing identity or conflicting records")


async def complete_ndc_stages(database, session, attempt):
    """Validate all input before copying deduplicated heaps, keys and relationship checks."""
    await _validate_ndc_inputs(database, attempt)
    for name, model in (("product", Product), ("package", Package)):
        candidate_table, input_table = attempt.tables[name], attempt.input_tables[name]
        key = tuple(model.__table__.primary_key.columns)[0].name
        relation = f'"{attempt.schema}"."{input_table.name}"'
        columns = tuple(column.name for column in model.__table__.columns)
        projected = ", ".join(f'"{column}"' for column in columns)
        await copy_projection(
            session,
            f'SELECT DISTINCT ON ("{key}") {projected} FROM {relation} ORDER BY "{key}"',
            schema_name=attempt.schema,
            table_name=candidate_table.name,
            columns=columns,
            max_bytes=32 * 1024**3,
            timeout=300,
        )
    counts_by_table = {}
    for name, model in (("product", Product), ("package", Package)):
        candidate_table = attempt.tables[name]
        await create_table_constraints(session, candidate_table)
        for statement in declared_index_statements(
            model,
            candidate_table,
            attempt.schema,
            model.__my_additional_indexes__,
            index_schema_name=candidate_table.name,
        ):
            await session.execute(statement)
        counts_by_table[name] = await database.scalar(
            text(f'SELECT count(*) FROM "{attempt.schema}"."{candidate_table.name}"')
        )
    if len(RELATIONSHIPS) != 1:
        raise RuntimeError("unsupported NDC relationship declaration")
    child_model, child_column, parent_model, parent_column, is_array, allows_null = RELATIONSHIPS[0]
    if is_array:
        raise RuntimeError("unsupported NDC relationship declaration")
    child = f'"{attempt.schema}"."{attempt.tables[child_model.__tablename__].name}"'
    parent = f'"{attempt.schema}"."{attempt.tables[parent_model.__tablename__].name}"'
    missing = f'NOT EXISTS(SELECT 1 FROM {parent} parent WHERE parent."{parent_column}"=child."{child_column}")'
    invalid = (
        f'child."{child_column}" IS NOT NULL AND {missing}'
        if allows_null
        else f'child."{child_column}" IS NULL OR {missing}'
    )
    if await database.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {child} child WHERE {invalid})")):
        raise RuntimeError("NDC package refers to an absent product")
    attempt.counts.update(counts_by_table)


async def _check_ndc_run_owner(database, attempt: NdcAttempt) -> None:
    run_id = await database.scalar(
        text(f"""
        SELECT run_id FROM {attempt.schema}.import_run
         WHERE run_id=:run_id AND importer='ndc' AND metrics->>'ndc_attempt_id'=:attempt_id AND status='running'
           AND (CAST(:node_id AS text) IS NULL OR (engine='drug-api' AND node_id=:node_id))
         FOR SHARE
    """),
        run_id=attempt.run_id,
        attempt_id=attempt.suffix,
        node_id=attempt.node_id,
    )
    if run_id != attempt.run_id:
        raise RuntimeError("NDC run ownership or status changed")


async def audit_ndc_tables(database, session, attempt: NdcAttempt, acquisition: dict) -> dict:
    """Hash exact locked native CSV bytes and require the full persisted census."""
    if sum(part["records"] for part in acquisition["partitions"]) != attempt.counts["source_products"]:
        raise RuntimeError("NDC acquired and persisted record census differs")
    await database.status("SET LOCAL DateStyle TO 'ISO, YMD'")
    connection = await session.connection()
    raw_connection = await connection.get_raw_connection()
    tables_by_name = {}
    for name, table in attempt.tables.items():
        tables_by_name[name] = await _audit_locked_ndc_table(
            database, raw_connection.driver_connection, attempt, name, table
        )
    return {
        "format": "ndc-publication-v1",
        "run_id": attempt.run_id,
        "attempt_id": attempt.suffix,
        "complete": acquisition["complete"],
        "acquisition": acquisition,
        "counts": dict(attempt.counts),
        "tables": tables_by_name,
    }


async def _audit_locked_ndc_table(database, driver, attempt: NdcAttempt, name: str, table) -> dict:
    return await audit_ndc_table(
        driver,
        schema=attempt.schema,
        table_name=table.name,
        name=name,
        expected_oid=attempt.table_oids[name],
        expected_count=attempt.counts[name],
    )


async def _lock_ndc_publication(database, schema: str) -> None:
    """Serialize cooperating publishers only for their final transaction."""
    acquired = await database.scalar(
        text("SELECT pg_try_advisory_xact_lock(hashtextextended(:scope, 0))"), scope=f"{schema}:ndc-publication"
    )
    if not acquired:
        raise RuntimeError("another NDC publication is active")


async def check_ndc_incumbents(database, attempt: NdcAttempt) -> None:
    """Fence the predecessor pair without blocking readers during handoff."""
    await _lock_ndc_publication(database, attempt.schema)
    lock_mode = "ACCESS SHARE" if attempt.publication_mode == "handoff" else "ACCESS EXCLUSIVE"
    for name, table_oid in attempt.incumbent_oids.items():
        if table_oid is not None:
            await database.status(f"LOCK TABLE {attempt.schema}.{name} IN {lock_mode} MODE NOWAIT")
    if await ndc_table_oids(database, attempt.schema) != attempt.incumbent_oids:
        raise RuntimeError("NDC incumbent changed during acquisition")


async def finish_ndc_publication(database, attempt: NdcAttempt, receipt: dict, *, published: bool) -> dict:
    """Record success and both physical bindings in the caller's swap transaction."""
    if published and attempt.publication_mode != "direct":
        raise RuntimeError("NDC direct publication is disabled for this attempt")
    expected_suffix = "" if published else attempt.suffix
    if await ndc_table_oids(database, attempt.schema, expected_suffix) != attempt.table_oids:
        raise RuntimeError("NDC final table identity differs from audited stages")
    phase = "ndc import published" if published else "ndc sample validated"
    receipt_dict = {**receipt, "published": published, "completed_at": datetime.datetime.now(datetime.UTC).isoformat()}
    metrics_dict = {
        "ndc_attempt_id": attempt.suffix,
        "ndc_publication" if published else "ndc_sample": receipt_dict,
        "source_product_count": attempt.counts["source_products"],
        "imported_product_count": attempt.counts["product"],
    }
    changed = await database.status(
        text(f"""
        UPDATE {attempt.schema}.import_run
           SET status='succeeded', phase_detail=:phase, heartbeat_at=CURRENT_TIMESTAMP,
               finished_at=CURRENT_TIMESTAMP, metrics=CAST(:metrics AS jsonb), error=NULL,
               progress=CAST(:progress AS jsonb)
         WHERE run_id=:run_id AND importer='ndc' AND metrics->>'ndc_attempt_id'=:attempt_id AND status='running'
           AND (CAST(:node_id AS text) IS NULL OR (engine='drug-api' AND node_id=:node_id))
    """),
        run_id=attempt.run_id,
        attempt_id=attempt.suffix,
        phase=phase,
        node_id=attempt.node_id,
        metrics=json.dumps(metrics_dict),
        progress=json.dumps(
            {
                "unit": "records",
                "done": attempt.counts["source_products"],
                "total": attempt.counts["source_products"],
                "pct": 100,
                "message": phase,
            }
        ),
    )
    if changed != 1:
        raise RuntimeError("NDC terminal success lost its run ownership fence")
    if published:
        comment = json.dumps(receipt_dict, sort_keys=True)
        for name in attempt.tables:
            await _comment_ndc_table(database, attempt.schema, name, comment)
        await database.status(
            "DROP TABLE " + ", ".join(f"{attempt.schema}.{table.name}" for table in attempt.input_tables.values())
        )
    else:
        await discard_ndc_stages(database, attempt)
    return receipt_dict


async def handoff_ndc_stages(database, attempt: NdcAttempt, receipt_dict: dict) -> dict:
    """Commit completed private stages, never aliases or a native success claim."""
    if attempt.publication_mode != "handoff" or not attempt.node_id:
        raise RuntimeError("NDC handoff requires explicit process authority")
    if receipt_dict.get("complete") is not True:
        raise RuntimeError("NDC handoff requires complete acquisition")
    if await ndc_table_oids(database, attempt.schema, attempt.suffix) != attempt.table_oids:
        raise RuntimeError("NDC handoff table identity differs from audited stages")
    await check_ndc_incumbents(database, attempt)
    handoff_by_field = {
        **receipt_dict,
        "format": "ndc-stage-handoff-v1",
        "schema": attempt.schema,
        "tables": {
            name: {**receipt_dict["tables"][name], "name": table.name} for name, table in attempt.tables.items()
        },
        "database_oid": await database.scalar(
            text("SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()")
        ),
        "import_run_oid": await database.scalar(
            text("SELECT to_regclass(:name)::oid"), name=f"{attempt.schema}.import_run"
        ),
        "incumbent_oids": dict(attempt.incumbent_oids),
        "published": False,
    }
    encoded = json.dumps(
        handoff_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()
    handoff_by_field["handoff_sha256"] = hashlib.sha256(encoded).hexdigest()
    handoff_by_field["handed_off_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    if any(type(count) is not int or not 0 < count < 2**63 for count in attempt.counts.values()):
        raise RuntimeError("NDC handoff requires a nonempty completed census")
    if len(json.dumps(handoff_by_field, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()) > 128 * 1024:
        raise RuntimeError("NDC handoff exceeds its evidence bound")
    await _record_ndc_handoff(database, attempt, handoff_by_field)
    marker = json.dumps(
        {key: handoff_by_field[key] for key in ("format", "run_id", "attempt_id", "handoff_sha256")}, sort_keys=True
    )
    for table in attempt.tables.values():
        await _comment_ndc_table(database, attempt.schema, table.name, marker)
    if attempt.input_oids:
        await database.status(
            "DROP TABLE " + ", ".join(f"{attempt.schema}.{table.name}" for table in attempt.input_tables.values())
        )
    return handoff_by_field


async def _record_ndc_handoff(database, attempt, handoff):
    """Transfer terminal responsibility under the actual attempt's active-run CAS."""
    changed = await database.status(
        text(f"""
        UPDATE {attempt.schema}.import_run
           SET status='finalizing', phase_detail='ndc stages awaiting publication',
               heartbeat_at=CURRENT_TIMESTAMP, finished_at=NULL, error=NULL,
               metrics=COALESCE(metrics, '{{}}'::jsonb) || CAST(:metrics AS jsonb)
         WHERE run_id=:run_id AND engine='drug-api' AND importer='ndc' AND node_id=:node_id
           AND metrics->>'ndc_attempt_id'=:attempt_id AND status='running'
           AND NOT (COALESCE(metrics, '{{}}'::jsonb) ?| ARRAY['ndc_handoff','ndc_publication','ndc_sample'])
           AND NOT COALESCE((params->>'test_mode')::boolean, false)
           AND NOT COALESCE((params->>'test')::boolean, false)
    """),
        run_id=attempt.run_id,
        attempt_id=attempt.suffix,
        metrics=json.dumps({"ndc_attempt_id": attempt.suffix, "ndc_handoff": handoff}),
        node_id=attempt.node_id,
    )
    if changed != 1:
        raise RuntimeError("NDC completed handoff lost its active run ownership fence")


async def fail_ndc_attempt(database, attempt: NdcAttempt) -> int:
    """Fail only this still-running owner; committed success can never be relabeled."""
    return await database.status(
        text(f"""
        UPDATE {attempt.schema}.import_run
           SET status='failed', phase_detail='ndc import failed', finished_at=CURRENT_TIMESTAMP,
               error='{{"code":"ndc_import_failed"}}'::jsonb
         WHERE run_id=:run_id AND importer='ndc' AND metrics->>'ndc_attempt_id'=:attempt_id AND status='running'
           AND (CAST(:node_id AS text) IS NULL OR (engine='drug-api' AND node_id=:node_id))
    """),
        run_id=attempt.run_id,
        attempt_id=attempt.suffix,
        node_id=attempt.node_id,
    )


async def _comment_ndc_table(database, schema: str, table_name: str, comment: str) -> None:
    quoted_comment = "'" + comment.replace("'", "''") + "'"
    await database.status(f"COMMENT ON TABLE {schema}.{table_name} IS {quoted_comment}")
