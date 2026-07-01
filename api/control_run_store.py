"""Database writes for import-control run records."""

import json
from typing import Any

from sqlalchemy import text

from db.models import db


async def insert_import_run(schema: str, run_record_dict: dict[str, Any]) -> None:
    """Insert the queued import-run record before work is enqueued."""
    insert_values_dict = {**run_record_dict, **_json_fields(run_record_dict)}
    await db.status(
        text(
            f"""
        INSERT INTO {schema}.import_run
            (run_id, engine, node_id, importer, family, status, phase_detail, params, idempotency_key,
             triggered_by, schedule_id, created_at, heartbeat_at, progress, metrics, error, import_id, retry_of_run_id)
        VALUES
            (:run_id, :engine, :node_id, :importer, :family, :status, :phase_detail, :params, :idempotency_key,
             :triggered_by, :schedule_id, :created_at, :heartbeat_at, :progress, :metrics, :error, :import_id, :retry_of_run_id)
        """
        ),
        **insert_values_dict,
    )


async def update_import_run_after_enqueue(
    schema: str,
    run_id: str,
    enqueue_update: dict[str, Any],
) -> None:
    """Persist queue metadata after ARQ accepts or rejects a run."""
    update_values_dict = {**enqueue_update, **_json_fields(enqueue_update)}
    await db.status(
        text(
            f"""
        UPDATE {schema}.import_run
           SET status = :status, phase_detail = :phase_detail, heartbeat_at = :heartbeat_at,
               progress = :progress, metrics = :metrics, error = :error
         WHERE run_id = :run_id
        """
        ),
        run_id=run_id,
        **update_values_dict,
    )


def _json_fields(values: dict[str, Any]) -> dict[str, str | None]:
    encoded_fields_dict: dict[str, str | None] = {}
    for key in ("params", "progress", "metrics", "error"):
        if key in values:
            value = values[key]
            encoded_fields_dict[key] = None if value is None else json.dumps(value)
    return encoded_fields_dict
