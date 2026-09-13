"""Durable, opt-in handoff of a completed local NDC table pair."""

import datetime
import hashlib
import json
import re

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from db.connection import current_session

HANDOFF_FORMAT = "ndc-stage-handoff-v1"
_FIELDS = {
    "format", "run_id", "attempt_id", "schema", "database_oid", "import_run_oid",
    "complete", "published", "handed_off_at", "handoff_sha256", "acquisition",
    "counts", "tables", "incumbent_oids",
}
_COUNTS = {"source_products", "source_packages", "product", "package", "batches"}


def require_handoff_transaction_owner(database=None) -> None:
    """Refuse to advertise durability from a borrowed caller transaction."""
    try:
        current_session()
    except RuntimeError:
        _require_handoff_engine(database)
        return
    raise RuntimeError("NDC handoff requires an unbound database session")


def _require_handoff_engine(database) -> None:
    factory = getattr(database, "session_factory", None)
    if factory is not None and (
            not isinstance(factory, async_sessionmaker) or not isinstance(database.engine, AsyncEngine)
            or factory.kw.get("bind") is not database.engine):
        raise RuntimeError("NDC handoff requires its own database engine binding")


def validate_publication_mode(mode: str) -> str:
    """Accept only the two closed local publication modes."""
    if mode not in {"native", "handoff"}:
        raise ValueError("unsupported NDC publication mode")
    return mode


def _candidate_digest(receipt_dict: dict) -> str:
    candidate_dict = {key: entry for key, entry in receipt_dict.items()
                      if key not in {"handed_off_at", "handoff_sha256"}}
    encoded = json.dumps(candidate_dict, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def _is_positive_oid(identity) -> bool:
    return type(identity) is int and 0 < identity <= 4294967295


def _has_valid_tables(receipt_dict: dict) -> bool:
    tables_by_name = receipt_dict["tables"]
    incumbents_by_name = receipt_dict["incumbent_oids"]
    if set(tables_by_name) != {"product", "package"} or set(incumbents_by_name) != set(tables_by_name):
        return False
    for name, table_dict in tables_by_name.items():
        if (set(table_dict) != {"name", "oid", "row_count", "size_bytes", "sha256", "encoding", "columns"}
                or table_dict["name"] != f"{name}_{receipt_dict['attempt_id']}"
                or not _is_positive_oid(table_dict["oid"])
                or table_dict["encoding"] != "postgres-copy-csv-v1"
                or not re.fullmatch(r"[0-9a-f]{64}", table_dict["sha256"])):
            return False
        if any(type(table_dict[key]) is not int or table_dict[key] < 0 for key in ("row_count", "size_bytes")):
            return False
        if table_dict["row_count"] != receipt_dict["counts"][name] or not _has_valid_columns(table_dict["columns"]):
            return False
        incumbent = incumbents_by_name[name]
        if incumbent is not None and not _is_positive_oid(incumbent):
            return False
    stage_oids = {table_dict["oid"] for table_dict in tables_by_name.values()}
    return len(stage_oids) == 2 and not stage_oids.intersection(incumbents_by_name.values())


def _has_valid_columns(columns) -> bool:
    if not isinstance(columns, list) or not columns:
        return False
    for column_dict in columns:
        if (not isinstance(column_dict, dict) or set(column_dict) != {"name", "type", "not_null"}
                or not isinstance(column_dict["name"], str) or not column_dict["name"]
                or not isinstance(column_dict["type"], str) or not column_dict["type"]
                or type(column_dict["not_null"]) is not bool):
            return False
    return len({column_dict["name"] for column_dict in columns}) == len(columns)


def has_valid_ndc_handoff(receipt_dict, run_id: str, attempt_id: str | None = None) -> bool:
    """Classify an exact native receipt; this is not a table ownership seal."""
    try:
        if (not isinstance(receipt_dict, dict) or set(receipt_dict) != _FIELDS
                or receipt_dict["format"] != HANDOFF_FORMAT or receipt_dict["run_id"] != run_id
                or not isinstance(run_id, str) or not run_id
                or not re.fullmatch(r"n[0-9a-f]{32}", receipt_dict["attempt_id"])
                or (attempt_id is not None and receipt_dict["attempt_id"] != attempt_id)
                or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", receipt_dict["schema"])
                or receipt_dict["complete"] is not True or receipt_dict["published"] is not False):
            return False
        if not all(_is_positive_oid(receipt_dict[key]) for key in ("database_oid", "import_run_oid")):
            return False
        counts_by_name = receipt_dict["counts"]
        if set(counts_by_name) != _COUNTS or any(type(count) is not int or count < 0 for count in counts_by_name.values()):
            return False
        if receipt_dict["acquisition"]["complete"] is not True or not _has_valid_tables(receipt_dict):
            return False
        if datetime.datetime.fromisoformat(receipt_dict["handed_off_at"]).tzinfo is None:
            return False
        return receipt_dict["handoff_sha256"] == _candidate_digest(receipt_dict)
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


async def _handoff_receipt(database, attempt, audit_dict: dict) -> dict:
    tables_by_name = {name: {**table_dict, "name": attempt.tables[name].name}
                      for name, table_dict in audit_dict["tables"].items()}
    if {name: table_dict["oid"] for name, table_dict in tables_by_name.items()} != attempt.table_oids:
        raise RuntimeError("NDC handoff differs from its locked table pair")
    receipt_dict = {
        **audit_dict, "format": HANDOFF_FORMAT, "schema": attempt.schema,
        "database_oid": await database.scalar("SELECT oid FROM pg_database WHERE datname=current_database()"),
        "import_run_oid": await database.scalar(text("SELECT to_regclass(:name)::oid"),
                                                name=f"{attempt.schema}.import_run"),
        "tables": tables_by_name, "incumbent_oids": dict(attempt.incumbent_oids), "published": False,
        "handed_off_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    receipt_dict["handoff_sha256"] = _candidate_digest(receipt_dict)
    if not has_valid_ndc_handoff(receipt_dict, attempt.run_id, attempt.suffix):
        raise RuntimeError("NDC handoff audit is incomplete or inconsistent")
    return receipt_dict


async def handoff_ndc_stages(database, attempt, audit_dict: dict) -> dict:
    """Transfer an audited, exclusively locked pair in the caller's owned transaction."""
    receipt_dict = await _handoff_receipt(database, attempt, audit_dict)
    changed = await database.status(text(f"""
        UPDATE {attempt.schema}.import_run
           SET status='finalizing', phase_detail='ndc stages awaiting publication',
               heartbeat_at=CURRENT_TIMESTAMP, finished_at=NULL, error=NULL,
               metrics=COALESCE(metrics, '{{}}'::jsonb) || jsonb_build_object('ndc_handoff', CAST(:receipt AS jsonb)),
               progress=CAST(:progress AS jsonb)
         WHERE run_id=:run_id AND engine='drug-api' AND importer='ndc' AND status='running'
           AND metrics->>'ndc_attempt_id'=:attempt_id
           AND NOT (COALESCE(metrics, '{{}}'::jsonb) ? 'ndc_handoff')
    """), run_id=attempt.run_id, attempt_id=attempt.suffix, receipt=json.dumps(receipt_dict),
        progress=json.dumps({"unit": "records", "done": attempt.counts["source_products"],
                             "total": attempt.counts["source_products"], "pct": 100,
                             "message": "awaiting publication"}))
    if changed != 1:
        raise RuntimeError("NDC handoff lost its running attempt")
    marker = json.dumps({"format": HANDOFF_FORMAT, "run_id": attempt.run_id,
                         "attempt_id": attempt.suffix, "handoff_sha256": receipt_dict["handoff_sha256"]}, sort_keys=True)
    quoted_marker = "'" + marker.replace("'", "''") + "'"
    for table in attempt.tables.values():
        await database.status(f"COMMENT ON TABLE {attempt.schema}.{table.name} IS {quoted_marker}")
    return receipt_dict


async def require_ndc_cleanup_owner(database, attempt) -> None:
    """Hold the native attempt fence while discarding only never-handed-off stages."""
    run_id = await database.scalar(text(f"""
        SELECT run_id FROM {attempt.schema}.import_run
         WHERE run_id=:run_id AND engine='drug-api' AND importer='ndc'
           AND metrics->>'ndc_attempt_id'=:attempt_id
           AND NOT (COALESCE(metrics, '{{}}'::jsonb) ? 'ndc_handoff')
         FOR SHARE
    """), run_id=attempt.run_id, attempt_id=attempt.suffix)
    if run_id != attempt.run_id:
        raise RuntimeError("NDC stages are handed off or no longer owned for cleanup")
