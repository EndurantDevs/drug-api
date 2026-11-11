"""DDL helpers for publishing staged FDA label import tables."""

import asyncio
import hashlib
import json
import os
import re
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import text

from db.connection import ConnectionProxy, _current_session_or_none
from process.result_publication_authority import (
    publish_local_result_generation,
    read_result_publication_authority,
    require_label_ordinary_publication,
    schema_name,
    validate_result_publication_authority,
)


async def publish_label_table(database: Any, db_schema: str, import_date: str) -> None:
    """Create indexes and swap the staged label table into service."""
    async with database.transaction():
        await require_label_ordinary_publication(database, db_schema)
        await database.status("SET LOCAL lock_timeout = '5s'")
        await database.status(f'LOCK TABLE "{db_schema}"."label_{import_date}" IN ACCESS EXCLUSIVE MODE')
        await read_result_publication_authority(database, importer_id="label", schema=db_schema, lock=True)
        print("Creating indexes..")
        await database.status(
            f"CREATE INDEX idx_product_ndc_{import_date} ON {db_schema}.label_{import_date} USING GIN(product_ndc);"
        )
        await database.status(
            f"CREATE INDEX idx_package_ndc_{import_date} ON {db_schema}.label_{import_date} USING GIN(package_ndc);"
        )
        await database.status(f"CREATE INDEX idx_label_id_{import_date} ON {db_schema}.label_{import_date} (id);")
        await database.status(
            f"CREATE INDEX idx_label_set_id_{import_date} ON {db_schema}.label_{import_date} (set_id);"
        )

        await database.status("SET LOCAL lock_timeout = '5s'")
        await database.status(f"DROP TABLE IF EXISTS {db_schema}.label_old;")

        await database.status(f"ALTER INDEX IF EXISTS {db_schema}.idx_product_ndc RENAME TO idx_product_ndc_old;")
        await database.status(f"ALTER INDEX IF EXISTS {db_schema}.idx_package_ndc RENAME TO idx_package_ndc_old;")
        await database.status(f"ALTER INDEX IF EXISTS {db_schema}.idx_label_id RENAME TO idx_label_id_old;")
        await database.status(f"ALTER INDEX IF EXISTS {db_schema}.idx_label_set_id RENAME TO idx_label_set_id_old;")
        await database.status(f"ALTER TABLE IF EXISTS {db_schema}.label RENAME TO label_old;")

        await database.status(
            f"ALTER INDEX IF EXISTS {db_schema}.idx_product_ndc_{import_date} RENAME TO idx_product_ndc;"
        )
        await database.status(
            f"ALTER INDEX IF EXISTS {db_schema}.idx_package_ndc_{import_date} RENAME TO idx_package_ndc;"
        )
        await database.status(f"ALTER INDEX IF EXISTS {db_schema}.idx_label_id_{import_date} RENAME TO idx_label_id;")
        await database.status(
            f"ALTER INDEX IF EXISTS {db_schema}.idx_label_set_id_{import_date} RENAME TO idx_label_set_id;"
        )
        await database.status(f"ALTER TABLE IF EXISTS {db_schema}.label_{import_date} RENAME TO label;")
        await publish_local_result_generation(
            database,
            importer_id="label",
            schema=db_schema,
            consumed_dependencies={},
        )


async def _label_state(database, schema):
    """Read the exact physical heap and its complete typed generation identity."""
    generation = await read_result_publication_authority(database, importer_id="label", schema=schema)
    oid = await database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{schema}.label")
    if generation.relation_oids is not None and generation.relation_oids != (oid,):
        raise RuntimeError("Label generation and canonical heap differ")
    return {"table_oid": oid, "generation": generation.as_dict()}


def require_label_completion_session():
    """Require transaction ownership before a coordinator can promise committed success."""
    session = _current_session_or_none()
    if session is not None:
        raise RuntimeError("Label completion requires its own top-level transaction")


def label_publication_intent(ctx, schema):
    """Bind a retained stage to its managed run before publication can start."""
    return {
        "format": "label-publication-intent-v1",
        "run_id": ctx.get("control_run_id"),
        "attempt_id": ctx["import_date"],
        "schema": schema,
        "stage_oid": ctx["label_stage_oid"],
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID") or None,
        "source_label_count": ctx["context"]["label_count"],
    }


def label_publication_protocol(run_id, schema, node_id):
    """Identify newly admitted attempts whose stage and intent commit together."""
    return {
        "format": "label-publication-protocol-v1",
        "run_id": run_id,
        "attempt_id": "l" + hashlib.sha256(run_id.encode()).hexdigest()[:32],
        "schema": schema_name(schema),
        "node_id": node_id,
    }


async def claim_label_run(database, ctx, run_id):
    """Atomically claim one admitted run using its actual dequeuing worker identity."""
    if not run_id:
        return
    require_label_completion_session()
    schema = schema_name(os.getenv("DB_SCHEMA") or "rx_data")
    claim_dict = _label_worker_claim(ctx, run_id)
    changed = await database.status(
        text(f"""
        UPDATE {schema}.import_run
           SET status='running', phase_detail='label worker claimed',
               started_at=COALESCE(started_at,CURRENT_TIMESTAMP), heartbeat_at=CURRENT_TIMESTAMP,
               error=NULL, metrics=COALESCE(metrics,'{{}}'::jsonb) || CAST(:metrics AS jsonb)
         WHERE run_id=:run_id AND engine='drug-api' AND importer='label'
           AND status IN ('queued','starting','running','finalizing')
           AND (CAST(:node_id AS text) IS NULL OR node_id=:node_id)
           AND ((NOT (COALESCE(metrics,'{{}}'::jsonb) ? 'label_worker_claim')
                 AND NOT (COALESCE(metrics,'{{}}'::jsonb) ? 'label_publication_intent')
                 AND NOT (COALESCE(metrics,'{{}}'::jsonb) ? 'label_publication'))
                OR (status='running' AND metrics->'label_worker_claim'=CAST(:claim AS jsonb)))
        """),
        run_id=run_id,
        node_id=os.getenv("HLTHPRT_IMPORT_NODE_ID") or None,
        metrics=json.dumps({"label_worker_claim": claim_dict}),
        claim=json.dumps(claim_dict),
    )
    if changed != 1:
        await _refuse_label_claim(database, schema, run_id, claim_dict)


def _label_worker_claim(ctx, run_id):
    """Snapshot the immutable image supplied with the actual worker's Job identity."""
    job_name = os.getenv("HLTHPRT_LABEL_WORKER_JOB_NAME") or None
    job_uid = os.getenv("HLTHPRT_LABEL_WORKER_JOB_UID") or None
    image = None
    if job_name or job_uid:
        image = os.getenv("HLTHPRT_WORKER_JOB_IMAGE") or None
        if not job_name or not job_uid or not _is_immutable_worker_image(image):
            raise RuntimeError("Label worker claim requires its exact immutable runtime image and Job identity")
    claim_id = ctx.get("label_worker_claim_id") or uuid.uuid4().hex
    ctx["label_worker_claim_id"] = claim_id
    return {
        "format": "label-worker-claim-v2",
        "run_id": run_id,
        "claim_id": claim_id,
        "arq_job_id": ctx.get("job_id"),
        "job_name": job_name,
        "job_uid": job_uid,
        "worker_image": image,
    }


def _is_immutable_worker_image(image):
    """Accept one explicit image digest without mutable tags or whitespace."""
    return type(image) is str and re.fullmatch(r"[^@\s]+@sha256:[0-9a-f]{64}", image) is not None


async def _refuse_label_claim(database, schema, run_id, claim_dict):
    """Honor cancellation only for the exact already-owned runtime claim."""
    canceled = await database.status(
        text(f"""
        UPDATE {schema}.import_run SET status='canceled', phase_detail='label import canceled',
               heartbeat_at=CURRENT_TIMESTAMP, finished_at=COALESCE(finished_at,CURRENT_TIMESTAMP), error=NULL
         WHERE run_id=:run_id AND engine='drug-api' AND importer='label' AND status='canceling'
           AND (CAST(:node_id AS text) IS NULL OR node_id=:node_id)
           AND metrics->'label_worker_claim'=CAST(:claim AS jsonb)
        """),
        run_id=run_id,
        node_id=os.getenv("HLTHPRT_IMPORT_NODE_ID") or None,
        claim=json.dumps(claim_dict),
    )
    if canceled == 1:
        raise asyncio.CancelledError("Label import was canceled before acquisition")
    raise RuntimeError("Label worker lost its active run ownership fence")


async def _record_label_success(database, schema, receipt, intent):
    """Commit the exact managed result inside the data and generation transaction."""
    if not receipt["run_id"]:
        return
    metrics_dict = {
        "label_attempt_id": receipt["attempt_id"],
        "label_publication": receipt,
        "source_label_count": receipt["source_label_count"],
        "imported_label_count": receipt["imported_label_count"],
    }
    count = receipt["imported_label_count"]
    changed = await database.status(
        text(f"""
        UPDATE {schema}.import_run
           SET status='succeeded', phase_detail='label import published', heartbeat_at=CURRENT_TIMESTAMP,
               finished_at=CURRENT_TIMESTAMP, error=NULL,
               metrics=COALESCE(metrics, '{{}}'::jsonb) || CAST(:metrics AS jsonb),
               progress=CAST(:progress AS jsonb)
         WHERE run_id=:run_id AND engine='drug-api' AND importer='label' AND status='running'
           AND NOT (COALESCE(metrics, '{{}}'::jsonb) ? 'label_publication')
           AND metrics->'label_publication_intent'=CAST(:intent AS jsonb)
           AND (CAST(:node_id AS text) IS NULL OR node_id=:node_id)
        """),
        run_id=receipt["run_id"],
        node_id=receipt["node_id"],
        metrics=json.dumps(metrics_dict),
        progress=json.dumps(
            {
                "unit": "records",
                "total": count,
                "done": count,
                "pct": 100,
                "message": "succeeded",
                "phase": "label import published",
            }
        ),
        intent=json.dumps(intent),
    )
    if changed != 1:
        run_row = await database.first(
            text(
                f"SELECT status FROM {schema}.import_run "
                "WHERE run_id=:run_id AND engine='drug-api' AND importer='label' "
                "AND (CAST(:node_id AS text) IS NULL OR node_id=:node_id) FOR UPDATE"
            ),
            run_id=receipt["run_id"],
            node_id=receipt["node_id"],
        )
        if run_row is not None and run_row["status"] in {"canceling", "canceled"}:
            raise asyncio.CancelledError("Label import was canceled before publication")
        raise RuntimeError("Label terminal success lost its active run ownership fence")


async def _commit_label_publication(database, ctx, schema, suffix):
    """Swap the verified heap, advance its generation and record managed success atomically."""
    async with database.transaction():
        await database.status("SET LOCAL lock_timeout = '5s'")
        await database.status(f'LOCK TABLE "{schema}"."label_{suffix}" IN ACCESS EXCLUSIVE MODE')
        oid = await database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{schema}.label_{suffix}")
        if type(oid) is not int or oid != ctx["label_stage_oid"]:
            raise RuntimeError("Label stage identity changed before publication")
        await read_result_publication_authority(database, importer_id="label", schema=schema, lock=True)
        ctx["label_publication_preimage"] = await _label_state(database, schema)
        count = await database.scalar(text(f'SELECT count(*) FROM "{schema}"."label_{suffix}"'))
        expected = ctx["context"]["label_count"]
        if type(expected) is not int or expected <= 0 or count != expected:
            raise ValueError("Imported Label count does not match the complete source count")
        await publish_label_table(database, schema, suffix)
        state = await _label_state(database, schema)
        if (
            state["table_oid"] != oid
            or state["generation"]["local_generation"]
            != ctx["label_publication_preimage"]["generation"]["local_generation"] + 1
        ):
            raise RuntimeError("Published Label heap differs from the verified stage")
        receipt_dict = {
            "format": "label-publication-v1",
            "run_id": ctx.get("control_run_id"),
            "attempt_id": suffix,
            "schema": schema,
            "table_oid": oid,
            "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID") or None,
            "source_label_count": expected,
            "imported_label_count": count,
            "generation": state["generation"],
            "published": True,
        }
        intent = label_publication_intent(ctx, schema)
        await _record_label_success(database, schema, receipt_dict, intent)
        ctx["label_publication_candidate"] = receipt_dict
        return receipt_dict


async def _reconcile_label_publication(database, ctx, schema, suffix, expected):
    """Lock through any pending COMMIT before classifying an exact postimage or preimage."""
    async with asyncio.timeout(15), database.session_factory() as session, session.begin():
        fresh = ConnectionProxy(session)
        await fresh.status("SET LOCAL lock_timeout = '10s'")
        await fresh.status("SET LOCAL statement_timeout = '10s'")
        await fresh.first(
            text(f"SELECT importer_id FROM {schema}.result_publication_authority WHERE importer_id='label' FOR SHARE")
        )
        if expected["run_id"]:
            run_row = await fresh.first(
                text(
                    f"SELECT status, metrics, phase_detail, finished_at, heartbeat_at, error "
                    f"FROM {schema}.import_run "
                    "WHERE run_id=:run_id AND engine='drug-api' AND importer='label' "
                    "AND (CAST(:node_id AS text) IS NULL OR node_id=:node_id) FOR SHARE"
                ),
                run_id=expected["run_id"],
                node_id=expected["node_id"],
            )
            if run_row is None:
                raise RuntimeError("Label publication run is unavailable during reconciliation")
            recorded = (run_row["metrics"] or {}).get("label_publication")
        else:
            run_row, recorded = None, None
        observed = await _label_state(fresh, schema)
        postimage_dict = {"table_oid": expected["table_oid"], "generation": expected["generation"]}
        if observed == postimage_dict and (
            run_row is None
            or (
                run_row["status"] == "succeeded"
                and recorded == expected
                and run_row["phase_detail"] == "label import published"
                and run_row["error"] is None
                and isinstance(run_row["finished_at"], datetime)
                and isinstance(run_row["heartbeat_at"], datetime)
            )
        ):
            return "succeeded"
        stage_oid = await fresh.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{schema}.label_{suffix}")
        if (
            observed == ctx["label_publication_preimage"]
            and stage_oid == expected["table_oid"]
            and (run_row is None or run_row["status"] in {"running", "canceling", "canceled"} and recorded is None)
        ):
            return "canceled" if run_row is not None and run_row["status"] in {"canceling", "canceled"} else "running"
        raise RuntimeError("Label publication commit outcome could not be confirmed")


async def _await_label_reconciliation(database, ctx, schema, suffix, expected):
    """Drain bounded fresh-session reconciliation even when the caller is canceled."""
    reconciliation = asyncio.create_task(_reconcile_label_publication(database, ctx, schema, suffix, expected))
    while True:
        try:
            return await asyncio.shield(reconciliation)
        except asyncio.CancelledError:
            if reconciliation.done():
                reconciliation.result()


async def complete_label_publication(database, ctx):
    """Return only committed completion, retaining uncertain outcomes for reconciliation."""
    require_label_completion_session()
    schema = schema_name(os.getenv("DB_SCHEMA") or "rx_data")
    suffix = schema_name(ctx["import_date"])
    ctx.pop("label_publication_candidate", None)
    ctx.pop("label_commit_unknown", None)
    try:
        receipt = await _commit_label_publication(database, ctx, schema, suffix)
    except BaseException:
        expected = ctx.get("label_publication_candidate")
        if expected is None:
            raise
        try:
            status = await _await_label_reconciliation(database, ctx, schema, suffix, expected)
        except BaseException:
            ctx["label_commit_unknown"] = True
            raise
        task = asyncio.current_task()
        if status == "canceled" or (status != "succeeded" and task is not None and task.cancelling()):
            raise asyncio.CancelledError("Label import was canceled before commit")
        if status != "succeeded":
            raise
        receipt = expected
    task = asyncio.current_task()
    while task is not None and task.cancelling():
        task.uncancel()
    return receipt


def _retained_label_intent(row, schema):
    """Validate the persisted attempt independently of a departed worker context."""
    intent = (row["metrics"] or {}).get("label_publication_intent")
    suffix = "l" + hashlib.sha256(row["run_id"].encode()).hexdigest()[:32]
    if (
        type(intent) is not dict
        or set(intent) != {"format", "run_id", "attempt_id", "schema", "stage_oid", "node_id", "source_label_count"}
        or intent["format"] != "label-publication-intent-v1"
        or intent["run_id"] != row["run_id"]
        or intent["attempt_id"] != suffix
        or intent["schema"] != schema
        or (intent["node_id"] is not None and intent["node_id"] != row["node_id"])
        or type(intent["stage_oid"]) is not int
        or intent["stage_oid"] <= 0
        or type(intent["source_label_count"]) is not int
        or intent["source_label_count"] <= 0
    ):
        raise RuntimeError("Label recovery requires the exact retained attempt identity")
    return intent


def _confirmed_label_recovery(run_row_dict, intent):
    """Accept a completed historical attempt only with its exact typed publication receipt."""
    receipt = (run_row_dict["metrics"] or {}).get("label_publication")
    expected_dict = {
        "format": "label-publication-v1",
        "run_id": intent["run_id"],
        "attempt_id": intent["attempt_id"],
        "schema": intent["schema"],
        "table_oid": intent["stage_oid"],
        "node_id": intent["node_id"],
        "source_label_count": intent["source_label_count"],
        "imported_label_count": intent["source_label_count"],
    }
    if (
        type(receipt) is not dict
        or any(receipt.get(key) != expected_value for key, expected_value in expected_dict.items())
        or receipt.get("published") is not True
        or run_row_dict["phase_detail"] != "label import published"
        or run_row_dict["error"] is not None
        or not isinstance(run_row_dict["finished_at"], datetime)
        or not isinstance(run_row_dict["heartbeat_at"], datetime)
    ):
        raise RuntimeError("Label recovery completion receipt differs")
    generation_dict = dict(receipt.get("generation") or {})
    serving = generation_dict.pop("serving_generation", None)
    authority = validate_result_publication_authority({**generation_dict, **(serving or {})})
    if (
        authority.as_dict() != receipt["generation"]
        or authority.importer_id != "label"
        or authority.relation_oids != (intent["stage_oid"],)
        or authority.local_generation <= 0
    ):
        raise RuntimeError("Label recovery generation receipt differs")


async def reconcile_managed_label_run(database, run_id):
    """Resolve one selected attempt under publication locks, preserving every retained stage."""
    require_label_completion_session()
    if type(run_id) is not str or not run_id:
        raise ValueError("Label recovery requires one managed run ID")
    schema = schema_name(os.getenv("DB_SCHEMA") or "rx_data")
    async with asyncio.timeout(15), database.transaction():
        await database.status("SET LOCAL lock_timeout = '10s'")
        await database.status("SET LOCAL statement_timeout = '10s'")
        await require_label_ordinary_publication(database, schema)
        await read_result_publication_authority(database, importer_id="label", schema=schema, lock=True)
        run_row_dict = await database.first(
            text(
                f"SELECT * FROM {schema}.import_run "
                "WHERE run_id=:run_id AND engine='drug-api' AND importer='label' FOR UPDATE"
            ),
            run_id=run_id,
        )
        if run_row_dict is None or (
            os.getenv("HLTHPRT_IMPORT_NODE_ID") and run_row_dict["node_id"] != os.getenv("HLTHPRT_IMPORT_NODE_ID")
        ):
            raise RuntimeError("Label recovery run identity differs")
        if (run_row_dict["metrics"] or {}).get("label_publication_intent") is None:
            return await _reconcile_before_stage(database, schema, run_row_dict)
        intent = _retained_label_intent(run_row_dict, schema)
        result_dict = {
            "run_id": run_id,
            "stage": f"{schema}.label_{intent['attempt_id']}",
            "stage_oid": intent["stage_oid"],
        }
        if run_row_dict["status"] == "succeeded":
            _confirmed_label_recovery(run_row_dict, intent)
            return {**result_dict, "status": "succeeded", "publication": "committed"}
        stage_oid = await database.scalar(text("SELECT to_regclass(:name)::oid"), name=result_dict["stage"])
        canonical_oid = await database.scalar(text("SELECT to_regclass(:name)::oid"), name=f"{schema}.label")
        if (
            run_row_dict["status"] not in {"running", "finalizing", "canceling", "failed", "canceled"}
            or (run_row_dict["metrics"] or {}).get("label_publication") is not None
            or stage_oid != intent["stage_oid"]
            or canonical_oid == stage_oid
        ):
            raise RuntimeError("Label recovery outcome remains unconfirmed")
        status = await _finish_unpublished_recovery(database, schema, run_row_dict, "label_publication_intent", intent)
        return {**result_dict, "status": status, "publication": "unpublished"}


async def _finish_unpublished_recovery(database, schema, run_row_dict, identity_field, identity):
    """Close one locked unpublished attempt while preserving its exact identity."""
    status = "canceled" if run_row_dict["status"] in {"canceling", "canceled"} else "failed"
    if run_row_dict["status"] not in {"failed", "canceled"}:
        changed = await database.status(
            text(
                f"UPDATE {schema}.import_run SET status=:status, "
                "phase_detail='label publication reconciled as unpublished', heartbeat_at=CURRENT_TIMESTAMP, "
                "finished_at=CURRENT_TIMESTAMP, error=CAST(:error AS jsonb) "
                "WHERE run_id=:run_id AND status=:previous "
                "AND metrics->CAST(:identity_field AS text)=CAST(:identity AS jsonb)"
            ),
            run_id=run_row_dict["run_id"],
            status=status,
            previous=run_row_dict["status"],
            identity_field=identity_field,
            identity=json.dumps(identity),
            error=json.dumps({"code": "label_publication_uncommitted", "message": "Label publication did not commit"}),
        )
        if changed != 1:
            raise RuntimeError("Label recovery run changed")
    return status


async def _reconcile_before_stage(database, schema, run_row_dict):
    """Require a new admission protocol, an ended exact worker and an absent stage."""
    from api.control_workers import _BY_IMPORTER, _kubernetes_worker_state

    protocol = (run_row_dict["metrics"] or {}).get("label_publication_protocol")
    expected = label_publication_protocol(run_row_dict["run_id"], schema, run_row_dict["node_id"])
    if protocol != expected or (run_row_dict["metrics"] or {}).get("label_publication") is not None:
        raise RuntimeError("Label recovery requires the exact retained attempt identity or admission protocol")
    claim, image = _retained_label_claim(run_row_dict)
    if os.getenv("HLTHPRT_WORKER_LAUNCHER", "process").strip().lower() != "kubernetes":
        raise RuntimeError("Label pre-stage recovery requires exact terminal worker evidence")
    worker = await asyncio.to_thread(
        _kubernetes_worker_state, _BY_IMPORTER["label"], claimed_job_name=claim["job_name"]
    )
    if (
        worker.get("launcher") != "kubernetes"
        or worker.get("job_name") != claim["job_name"]
        or worker.get("terminal_status") not in {"failed", "succeeded"}
        or worker.get("active_jobs") != 0
        or worker.get("running") is not False
        or worker.get("job_uid") != claim["job_uid"]
        or worker.get("worker_image") != image
    ):
        raise RuntimeError("Label pre-stage recovery requires exact terminal worker evidence")
    stage = f"{schema}.label_{protocol['attempt_id']}"
    if await database.scalar(text("SELECT to_regclass(:name)::oid"), name=stage) is not None:
        raise RuntimeError("Label pre-stage recovery found a retained stage without its intent")
    if run_row_dict["status"] not in {"queued", "starting", "running", "finalizing", "canceling", "failed", "canceled"}:
        raise RuntimeError("Label recovery outcome remains unconfirmed")
    status = await _finish_unpublished_recovery(database, schema, run_row_dict, "label_publication_protocol", protocol)
    return {
        "run_id": run_row_dict["run_id"],
        "stage": stage,
        "stage_oid": None,
        "status": status,
        "publication": "unpublished",
    }


def _retained_label_claim(run_row_dict):
    """Read a closed claim, preserving the narrower historical image check for v1."""
    claim = (run_row_dict["metrics"] or {}).get("label_worker_claim")
    fields = {"format", "run_id", "claim_id", "arq_job_id", "job_name", "job_uid"}
    if (
        type(claim) is not dict
        or claim.get("run_id") != run_row_dict["run_id"]
        or any(type(claim.get(key)) is not str or not claim[key] for key in fields - {"format", "run_id"})
    ):
        raise RuntimeError("Label pre-stage recovery requires the exact worker claim")
    if claim.get("format") == "label-worker-claim-v1" and set(claim) == fields:
        image = os.getenv("HLTHPRT_WORKER_JOB_IMAGE")
    elif claim.get("format") == "label-worker-claim-v2" and set(claim) == fields | {"worker_image"}:
        image = claim["worker_image"]
    else:
        raise RuntimeError("Label pre-stage recovery requires the exact worker claim")
    if not _is_immutable_worker_image(image):
        raise RuntimeError("Label pre-stage recovery requires exact terminal worker evidence")
    return claim, image
