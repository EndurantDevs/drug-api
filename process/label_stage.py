"""Retain a completed Label attempt for the protected publication owner."""

import asyncio
import json
import logging
import os
import re
import uuid
from types import SimpleNamespace

from sqlalchemy import MetaData, text

from db.models import Label
from process.control_lifecycle import ensure_import_run_table
from process.ext.utils import download_it
from process.import_status_events import enqueue_status_event
from process.live_progress import enqueue_live_progress
from process.partition_download import download_partition_content
from process.result_publication_authority import require_label_ordinary_publication, schema_name


def validate_label_run_id(value):
    """Keep native run identities within the protected publication contract."""
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}", value):
        raise ValueError("Label run_id must be a machine identifier of at most 128 characters")
    return value


async def is_label_publication_protected(database, schema):
    """Use the same ownership check at startup and at the publication boundary."""
    try:
        await require_label_ordinary_publication(database, schema)
    except RuntimeError as exc:
        if str(exc) != "ordinary Label publication is disabled for the protected live table":
            raise
        return True
    return False


async def protected_label_import(ctx, task, database, project_row, download_spec):
    """Return false for ordinary publication; protected imports stop at finalizing."""
    schema = schema_name(os.getenv("DB_SCHEMA") or "rx_data")
    if not await is_label_publication_protected(database, schema):
        return False
    run_id = task.get("run_id")
    validate_label_run_id(run_id)
    if task.get("test_mode") or task.get("test"):
        raise ValueError("protected Label publication requires a complete source")
    if not 1 <= int(os.getenv("SAVE_PER_PACK", "100")) <= 1000:
        raise ValueError("protected Label batch size must be between 1 and 1000")
    ctx.setdefault("context", {})["label_protected_mode"] = True
    await ensure_import_run_table()
    suffix = "l" + uuid.uuid4().hex
    table = f"{schema}.label_{suffix}"
    stage = Label.__table__.to_metadata(MetaData(), schema=schema, name=f"label_{suffix}")
    identity_dict = {"format": "label-stage-v1", "run_id": run_id, "attempt_id": suffix}
    attempt = SimpleNamespace(
        schema=schema,
        run_id=run_id,
        suffix=suffix,
        table=table,
        stage=stage,
        owner_comment=json.dumps(identity_dict, sort_keys=True),
        oid=None,
    )
    try:
        await _claim_label_stage(database, attempt)
        expected, partitions = await _read_label_manifest()
        await _save_label_partitions(database, attempt, project_row, download_spec, partitions)
        receipt_dict = await finish_label_stage(
            database, schema, run_id, suffix, attempt.oid, attempt.owner_comment, expected
        )
    except BaseException:
        await _reconcile_failed_label_stage(database, attempt)
        raise
    try:
        enqueue_status_event(
            {
                "run_id": run_id,
                "importer": "label",
                "status": "finalizing",
                "metrics": {"label_attempt_id": suffix, "label_completed_stage": receipt_dict},
            }
        )
    except Exception:
        logging.getLogger(__name__).warning("Label completion event unavailable; finalizing receipt retained")
    return receipt_dict


async def _claim_label_stage(database, attempt):
    """Atomically claim the run and create its attempt-bound heap."""
    async with database.transaction() as session:
        await database.status("SET LOCAL lock_timeout = '5s'")
        await database.status("SET LOCAL statement_timeout = '120s'")
        changed = await database.status(
            text(f"""
            UPDATE {attempt.schema}.import_run
            SET metrics=COALESCE(metrics, '{{}}'::jsonb) || jsonb_build_object('label_attempt_id', CAST(:attempt_id AS text))
            WHERE run_id=:run_id AND importer='label' AND status IN ('queued','starting','running')
              AND NOT (COALESCE(metrics, '{{}}'::jsonb) ? 'label_attempt_id')
        """),
            run_id=attempt.run_id,
            attempt_id=attempt.suffix,
        )
        if changed != 1:
            raise RuntimeError("Label attempt is no longer claimable")
        connection = await session.connection()
        await connection.run_sync(attempt.stage.create)
        attempt.oid = await database.scalar(text("SELECT to_regclass(:name)::oid"), name=attempt.table)
        if not attempt.oid:
            raise RuntimeError("Label stage is missing")
        quoted = attempt.owner_comment.replace("'", "''")
        await database.status(f"COMMENT ON TABLE {attempt.table} IS '{quoted}'")
        recorded = await database.status(
            text(f"""
            UPDATE {attempt.schema}.import_run
            SET metrics=metrics || jsonb_build_object('label_stage_oid', CAST(:oid AS bigint))
            WHERE run_id=:run_id AND metrics->>'label_attempt_id'=:attempt_id
        """),
            run_id=attempt.run_id,
            attempt_id=attempt.suffix,
            oid=attempt.oid,
        )
        if recorded != 1:
            raise RuntimeError("Label stage identity could not be recorded")


async def _read_label_manifest():
    """Validate the source census before consuming any partition."""
    response = await download_it(os.environ["HLTHPRT_MAIN_RX_JSON_URL"])
    response.raise_for_status()
    manifest_dict = json.loads(response.content)["results"]["drug"]["label"]
    expected = manifest_dict["total_records"]
    partitions = manifest_dict["partitions"]
    if type(expected) is not int or expected <= 0 or not partitions:
        raise ValueError("Label source census is missing")
    if any(type(part.get("records")) is not int or part["records"] <= 0 for part in partitions):
        raise ValueError("Label partition census is missing")
    if sum(part["records"] for part in partitions) != expected:
        raise ValueError("Label manifest census differs")
    return expected, partitions


async def _save_label_partitions(database, attempt, project_row, download_spec, partitions):
    """Await each existing downloader batch and verify its saved census."""
    columns = list(attempt.stage.columns.keys())
    saved_counts = [0]

    async def _consume(_job_name, batch):
        rows = [project_row(record, columns) for record in batch["results"]]
        if rows:
            async with database.transaction() as session:
                await lock_label_stage(
                    database, attempt.schema, attempt.run_id, attempt.suffix, attempt.oid, attempt.owner_comment
                )
                await session.execute(attempt.stage.insert(), rows)
            saved_counts[0] += len(rows)

    # Await the downloader's batches inline; completion means every insert finished.
    download_context_dict = {"redis": SimpleNamespace(enqueue_job=_consume)}
    for partition in partitions:
        before = saved_counts[0]
        await download_partition_content(
            download_context_dict,
            {
                "file": partition["file"],
                "run_id": attempt.run_id,
                "partition_records": partition["records"],
            },
            download_spec,
        )
        if saved_counts[0] - before != partition["records"]:
            raise RuntimeError("Label partition census differs from saved rows")


async def _reconcile_failed_label_stage(database, attempt):
    """Fail the owned run and discard only a stage that was never handed off."""
    failure_progress_dict = {"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "failed"}
    failure_error_dict = {"code": "label_stage_failed"}
    changed = 0
    try:
        async with asyncio.timeout(10):
            changed = await database.status(
                text(f"""
                UPDATE {attempt.schema}.import_run SET status='failed', finished_at=CURRENT_TIMESTAMP,
                    heartbeat_at=CURRENT_TIMESTAMP, phase_detail='label stage failed',
                    progress=CAST(:progress AS jsonb), error=CAST(:error AS jsonb)
                WHERE run_id=:run_id AND importer='label' AND metrics->>'label_attempt_id'=:attempt_id
                  AND status IN ('queued','starting','running')
            """),
                run_id=attempt.run_id,
                attempt_id=attempt.suffix,
                progress=json.dumps(failure_progress_dict),
                error=json.dumps(failure_error_dict),
            )
            if changed == 1 and attempt.oid is not None:
                await has_discarded_failed_label_stage(
                    database, attempt.schema, attempt.run_id, attempt.suffix, attempt.oid, attempt.owner_comment
                )
    except Exception:
        logging.getLogger(__name__).warning("Label failure reconciliation unavailable; owned stage retained")
    if changed == 1:
        try:
            enqueue_live_progress(
                run_id=attempt.run_id,
                importer="label",
                status="failed",
                phase="label stage failed",
                **failure_progress_dict,
                publish_event=False,
            )
        except Exception:
            logging.getLogger(__name__).warning("Label failure live progress unavailable; durable state retained")
        try:
            enqueue_status_event(
                {
                    "run_id": attempt.run_id,
                    "importer": "label",
                    "status": "failed",
                    "phase_detail": "label stage failed",
                    "progress": failure_progress_dict,
                    "error": failure_error_dict,
                }
            )
        except Exception:
            logging.getLogger(__name__).warning("Label failure event unavailable; durable state retained")


async def has_discarded_failed_label_stage(database, schema, run_id, suffix, oid, owner_comment):
    """Drop only a failed producer-owned heap that never emitted an admissible handoff."""
    schema = schema_name(schema)
    if not re.fullmatch(r"l[0-9a-f]{32}", suffix):
        raise ValueError("invalid Label attempt identifier")
    table = f"{schema}.label_{suffix}"
    async with database.transaction():
        await database.status("SET LOCAL lock_timeout = '2s'")
        await database.status("SET LOCAL statement_timeout = '5s'")
        if await database.scalar(text("SELECT to_regclass(:name)::oid"), name=table) is None:
            return False
        await database.status(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
        identity = await database.first(
            text("""
            SELECT c.oid, obj_description(c.oid,'pg_class') AS comment,
                   c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AS owned
            FROM pg_class c WHERE c.oid=to_regclass(:name)
        """),
            name=table,
        )
        if not identity or identity["oid"] != oid or identity["comment"] != owner_comment or not identity["owned"]:
            raise RuntimeError("Label failed-stage identity or owner changed")
        # The controller accepts only finalizing rows with label_completed_stage;
        # it holds this same run-row lock through admission and publication.
        run = await database.scalar(
            text(f"""
            SELECT run_id FROM {schema}.import_run
            WHERE run_id=:run_id AND importer='label' AND status='failed'
              AND metrics->>'label_attempt_id'=:attempt_id
              AND metrics->>'label_stage_oid'=:stage_oid
              AND NOT (metrics ? 'label_completed_stage') AND NOT (metrics ? 'label_publication')
              AND NOT (metrics ? 'label_publication_admission')
            FOR UPDATE NOWAIT
        """),
            run_id=run_id,
            attempt_id=suffix,
            stage_oid=str(oid),
        )
        if run != run_id:
            return False
        await database.status(f"DROP TABLE {table}")
    return True


async def lock_label_stage(database, schema, run_id, suffix, oid, owner_comment):
    """Fence every write and handoff against cancellation, replacement and stale attempts."""
    table = f"{schema}.label_{suffix}"
    await database.status("SET LOCAL lock_timeout = '5s'")
    await database.status(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    current = await database.scalar(text("SELECT to_regclass(:name)::oid"), name=table)
    comment = await database.scalar(text("SELECT obj_description(:oid, 'pg_class')"), oid=current)
    if current != oid or comment != owner_comment:
        raise RuntimeError("Label staging identity changed")
    owner = await database.scalar(
        text(f"""
        SELECT run_id FROM {schema}.import_run
        WHERE run_id=:run_id AND importer='label' AND metrics->>'label_attempt_id'=:attempt_id
          AND status IN ('queued','starting','running') FOR UPDATE
    """),
        run_id=run_id,
        attempt_id=suffix,
    )
    if owner != run_id:
        raise RuntimeError("Label run ownership or status changed")


async def finish_label_stage(database, schema, run_id, suffix, oid, owner_comment, expected):
    """Seal exact physical and logical evidence with the finalizing CAS."""
    table = f"{schema}.label_{suffix}"
    async with database.transaction():
        await lock_label_stage(database, schema, run_id, suffix, oid, owner_comment)
        count = await database.scalar(f"SELECT count(*) FROM {table}")
        if count != expected:
            raise RuntimeError("Label stage census differs from source")
        for column, name, method in (
            ("product_ndc", "product_ndc", "gin"),
            ("package_ndc", "package_ndc", "gin"),
            ("id", "label_id", "btree"),
            ("set_id", "label_set_id", "btree"),
        ):
            await database.status(f"CREATE INDEX idx_{name}_{suffix} ON {table} USING {method} ({column})")
        indexes = await database.all(
            text("""
            SELECT indexrelid::bigint AS oid, pg_get_indexdef(indexrelid) AS definition,
                   indisprimary AS primary,
                   indisvalid AS valid, indisready AS ready
            FROM pg_index WHERE indrelid=:oid ORDER BY indisprimary DESC, pg_get_indexdef(indexrelid)
        """),
            oid=oid,
        )
        if (
            len(indexes) != 5
            or sum(bool(index["primary"]) for index in indexes) != 1
            or any(not index["valid"] or not index["ready"] for index in indexes)
        ):
            raise RuntimeError("Label stage index validation failed")
        receipt_dict = {
            "format": "label-completed-stage-v1",
            "run_id": run_id,
            "attempt_id": suffix,
            "schema": schema,
            "table": f"label_{suffix}",
            "relation_oid": oid,
            "row_count": count,
            "source_row_count": expected,
            "complete": True,
            "indexes": [dict(index) for index in indexes],
        }
        changed = await database.status(
            text(f"""
            UPDATE {schema}.import_run SET status='finalizing', heartbeat_at=CURRENT_TIMESTAMP,
                phase_detail='label stage ready for protected publication',
                progress=jsonb_build_object('unit','stage','total',1,'done',1,'pct',100,
                    'message','stage complete; awaiting protected publication'),
                metrics=metrics || jsonb_build_object('label_completed_stage', CAST(:receipt AS jsonb))
            WHERE run_id=:run_id AND importer='label' AND metrics->>'label_attempt_id'=:attempt_id
              AND status IN ('queued','starting','running')
        """),
            run_id=run_id,
            attempt_id=suffix,
            receipt=json.dumps(receipt_dict),
        )
        if changed != 1:
            raise RuntimeError("Label finalizing lost its run ownership fence")
    return receipt_dict
