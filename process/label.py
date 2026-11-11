import asyncio
import datetime
import hashlib
import logging
import os
import uuid
from json import dumps, loads
from types import SimpleNamespace

import msgpack
from arq import create_pool
from dateutil.parser import parse as parse_date
from sqlalchemy import text
from sqlalchemy.inspection import inspect

from db.connection import init_db
from db.models import Label, db
from process.control_lifecycle import (
    _emit_control_run_transition,
    _flush_terminal_status_events,
    _record_control_run_transition,
    mark_control_run,
)
from process.ext.utils import download_it, make_class, print_time_info, push_objects
from process.import_status_events import enqueue_status_event
from process.label_publish import (
    claim_label_run,
    complete_label_publication,
    label_publication_intent,
    reconcile_managed_label_run,
    require_label_completion_session,
)
from process.live_progress import enqueue_live_progress
from process.partition_download import PartitionDownloadSpec, download_partition_content
from process.redis_config import redis_settings
from process.result_publication_authority import require_label_ordinary_publication, schema_name

logger = logging.getLogger(__name__)

LABEL_QUEUE_NAME = (
    os.environ.get("HLTHPRT_ARQ_QUEUE_LABEL") or os.environ.get("ARQ_QUEUE_LABEL") or "arq:queue:drug-api-import-label"
)
LABEL_DOWNLOAD_SPEC = PartitionDownloadSpec(
    importer="label",
    model="label",
    result_job_name="process_label_results",
    start_message="Starting Labels file download: ",
    item_label="labels",
)


async def download_label_content(ctx, task):
    """Await every saved batch before reading more of one FDA partition."""
    _require_label_attempt(ctx, task)
    saved_counts = [0]

    async def consume_batch(_job_name, batch):
        """Finish this batch before accepting more source records."""
        await process_label_results(ctx, batch)
        saved_counts[0] += len(batch["results"])

    download_context_dict = {**ctx, "redis": SimpleNamespace(enqueue_job=consume_batch)}
    await download_partition_content(download_context_dict, task, LABEL_DOWNLOAD_SPEC)
    if saved_counts[0] != task["partition_records"]:
        raise ValueError("Label partition census differs from saved records")
    return saved_counts[0]


def _require_label_attempt(ctx, task):
    if not ctx.get("import_date") or not isinstance(ctx.get("context"), dict):
        raise RuntimeError("Label batches require an active import coordinator")
    if task.get("run_id") != ctx.get("control_run_id"):
        raise RuntimeError("Label batch belongs to another import run")


def _label_row_dict_from_record(label_record: dict, label_columns: list[str]) -> dict[str, object]:
    label_row_dict: dict[str, object] = {}
    for label_column in label_columns:
        if label_column == "openfda":
            continue
        if label_column in ("product_ndc", "package_ndc"):
            label_row_dict[label_column] = (label_record.get("openfda") or {}).get(label_column, [])
            continue
        label_row_dict[label_column] = _label_column_value(label_record, label_column)
    return label_row_dict


def _label_column_value(label_record: dict, label_column: str) -> object:
    raw_value = label_record.get(label_column)
    if ("_date" in label_column) and raw_value:
        return parse_date(raw_value, fuzzy=True)
    if isinstance(raw_value, list):
        return ("\n".join(raw_value)).strip()
    if raw_value:
        return raw_value
    return None


async def process_label_results(ctx, task):
    """Normalize FDA label records and save them in this coordinator's stage."""
    _require_label_attempt(ctx, task)
    import_date = ctx["import_date"]
    ctx["context"]["run"] += 1
    run_id = task.get("run_id") or ctx.get("control_run_id") or ctx.get("context", {}).get("control_run_id")
    mylabel = make_class(Label, import_date)

    label_columns = [column.name for column in inspect(mylabel).c]
    label_rows = [_label_row_dict_from_record(label_record, label_columns) for label_record in task["results"]]

    await push_objects(label_rows, mylabel)
    enqueue_live_progress(
        run_id=run_id,
        importer="label",
        status="running",
        phase="label saving records",
        unit="records",
        done=task.get("batch_end") or len(task.get("results") or []),
        total=task.get("partition_records") or None,
        message=f"saved {len(label_rows)} labels",
    )


async def label_startup(ctx):
    """Connect without adopting or dropping another import's stage."""
    await init_db(db, asyncio.get_running_loop())


async def label_shutdown(ctx):
    """Close the pool after the coordinator has completed or failed."""
    await db.disconnect()


async def _label_shutdown_impl(ctx):
    """Report only the complete result already committed with its serving generation."""
    receipt = await complete_label_publication(db, ctx)
    _announce_label_success(ctx, receipt)
    return receipt


def _announce_label_success(ctx, receipt):
    """Project committed completion without changing its database outcome."""
    try:
        print("Labeling rows in JSON:", receipt["source_label_count"])
        print("Labeling rows in DB:", receipt["imported_label_count"])
        print_time_info(ctx["context"]["start"])
        progress_dict = {
            "unit": "records",
            "total": receipt["source_label_count"],
            "done": receipt["imported_label_count"],
            "pct": 100,
            "message": "succeeded",
            "phase": "label import published",
        }
        enqueue_status_event(
            {
                "run_id": receipt["run_id"],
                "importer": "label",
                "status": "succeeded",
                "phase_detail": "label import published",
                "progress": progress_dict,
                "metrics": {"label_publication": receipt},
            }
        )
        enqueue_live_progress(run_id=receipt["run_id"], importer="label", status="succeeded", **progress_dict)
    except Exception, asyncio.CancelledError:
        logger.warning("Label completion projection is unavailable after committed database success")


async def init_label_file(ctx, task=None):
    """Await the complete source and publication in one stage-owning control job."""
    require_label_completion_session()
    task = task if isinstance(task, dict) else {}
    ctx.pop("label_commit_unknown", None)
    run_id = task.get("run_id") or ctx.get("control_run_id")
    await claim_label_run(db, ctx, run_id)
    identity = str(run_id or ctx.get("job_id") or uuid.uuid4().hex)
    import_date = "l" + hashlib.sha256(identity.encode()).hexdigest()[:32]
    import_context_dict = {
        **ctx,
        "control_run_id": run_id,
        "import_date": import_date,
        "context": {"start": datetime.datetime.now(), "run": 0, "label_count": 0, "control_run_id": run_id},
    }
    try:
        await _acquire_label_source(import_context_dict, task)
        return await _label_shutdown_impl(import_context_dict)
    except BaseException as exc:
        if import_context_dict.get("label_stage_oid"):
            logger.warning(
                "Retained Label stage label_%s (OID %s) for run %s",
                import_date,
                import_context_dict["label_stage_oid"],
                run_id,
            )
        if import_context_dict.get("label_commit_unknown"):
            ctx["label_commit_unknown"] = True
            logger.warning("Label publication outcome unknown; attempt retained for reconciliation")
            raise
        try:
            await mark_control_run(
                run_id,
                status="canceled" if isinstance(exc, asyncio.CancelledError) else "failed",
                phase_detail="label import interrupted",
                progress_message="interrupted",
                error={"code": "import_failed", "message": str(exc)},
            )
        except Exception as mark_exc:
            logger.warning("failed to mark label import failure: %s", mark_exc)
        raise


async def _download_label_manifest():
    """Download and validate the complete Label partition census."""
    response_content = await download_it(os.environ["HLTHPRT_MAIN_RX_JSON_URL"])
    response_content.raise_for_status()
    manifest_dict = loads(response_content.content)
    manifest = manifest_dict["results"]["drug"]["label"]
    partitions = manifest["partitions"]
    total = manifest["total_records"]
    if (
        type(total) is not int
        or total <= 0
        or not isinstance(partitions, list)
        or not partitions
        or any(
            not isinstance(part, dict)
            or type(part.get("records")) is not int
            or part["records"] <= 0
            or not isinstance(part.get("file"), str)
            or not part["file"]
            for part in partitions
        )
        or sum(part["records"] for part in partitions) != total
        or len({part["file"] for part in partitions}) != len(partitions)
    ):
        raise ValueError("Label manifest and partition census disagree")
    return manifest


async def _record_label_acquisition(ctx, db_schema, partition_count):
    """Persist the exact retained attempt before consuming source partitions."""
    control_run_id = ctx.get("control_run_id") or ctx.get("context", {}).get("control_run_id")
    projection = await _record_control_run_transition(
        control_run_id,
        status="running",
        phase_detail="label partitions selected",
        progress_message=f"selected {partition_count} partition(s)",
        metrics={
            "source_label_count": ctx["context"]["label_count"],
            "partition_count": partition_count,
            "label_publication_intent": label_publication_intent(ctx, db_schema),
        },
        progress={
            "unit": "records",
            "total": ctx["context"]["label_count"],
            "done": 0,
            "pct": 0,
            "message": f"selected {partition_count} partition(s)",
        },
    )
    if control_run_id:
        recorded = await db.scalar(
            text(
                f"SELECT status='running' AND "
                f"metrics->'label_publication_intent'=CAST(:intent AS jsonb) FROM {db_schema}.import_run "
                "WHERE run_id=:run_id AND engine='drug-api' AND importer='label'"
            ),
            run_id=control_run_id,
            intent=dumps(label_publication_intent(ctx, db_schema)),
        )
        if recorded is not True:
            raise RuntimeError("Label retained attempt was not recorded")
    return control_run_id, projection


async def _prepare_label_stage(ctx, db_schema, partition_count):
    """Commit a new stage and its managed identity in the same transaction."""
    stage = make_class(Label, ctx["import_date"])
    async with db.transaction() as session:
        await require_label_ordinary_publication(db, db_schema)
        connection = await session.connection()
        if stage.__table__.schema:
            quoted_schema = connection.dialect.identifier_preparer.quote_schema(stage.__table__.schema)
            await connection.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
        await connection.run_sync(stage.__table__.create, checkfirst=False)
        ctx["label_stage_oid"] = await db.scalar(
            text("SELECT to_regclass(:name)::oid"), name=f"{db_schema}.{stage.__tablename__}"
        )
        if type(ctx["label_stage_oid"]) is not int or ctx["label_stage_oid"] <= 0:
            raise RuntimeError("Label stage identity is unavailable")
        control_run_id, projection = await _record_label_acquisition(ctx, db_schema, partition_count)
    if projection is not None:
        try:
            _emit_control_run_transition(*projection)
        except Exception:
            logger.warning("Label acquisition progress is unavailable after committed stage registration")
    return control_run_id


async def _acquire_label_source(ctx, task):
    """Acquire every selected partition into one new stage before publication."""
    test_mode = bool(task.get("test_mode") or task.get("test"))
    max_records = int(task.get("max_records") or os.environ.get("HLTHPRT_DRUG_IMPORT_TEST_MAX_RECORDS") or 5000)
    if max_records <= 0 or not 1 <= int(os.getenv("SAVE_PER_PACK", "100")) <= 1000:
        raise ValueError("Label sample and batch sizes must be positive, with batches at most 1000")
    manifest = await _download_label_manifest()
    partitions = manifest["partitions"]
    total = manifest["total_records"]
    if test_mode:
        partitions = partitions[:1]
        ctx["context"]["label_count"] = min(max_records, partitions[0]["records"])
    else:
        ctx["context"]["label_count"] = total
    db_schema = schema_name(os.getenv("DB_SCHEMA") or "rx_data")
    control_run_id = await _prepare_label_stage(ctx, db_schema, len(partitions))
    print(f"Going to import {ctx['context']['label_count']} rows")
    for partition_index, part in enumerate(partitions, start=1):
        partition_task_dict = {
            "what": "label",
            "file": part["file"],
            "run_id": control_run_id,
            "partition_records": min(max_records, int(part.get("records") or max_records))
            if test_mode
            else int(part.get("records") or 0),
            "partition_index": partition_index,
            "partition_count": len(partitions),
        }
        if test_mode:
            partition_task_dict["max_records"] = max_records
        await download_label_content(ctx, partition_task_dict)


async def main():
    """Enqueue the default label import manifest task."""
    redis = await create_pool(
        redis_settings(),
        default_queue_name=LABEL_QUEUE_NAME,
        job_serializer=msgpack.packb,
        job_deserializer=lambda b: msgpack.unpackb(b, raw=False),
    )
    await redis.enqueue_job("init_label_file")


async def reconcile_label_run(run_id):
    """Reconcile one operator-selected managed attempt and report its retained stage."""
    require_label_completion_session()
    await init_db(db, asyncio.get_running_loop())
    try:
        result = await reconcile_managed_label_run(db, run_id)
        try:
            enqueue_status_event(
                {
                    "run_id": run_id,
                    "importer": "label",
                    "status": result["status"],
                    "phase_detail": "label publication reconciled",
                }
            )
            await _flush_terminal_status_events()
        except Exception:
            logger.warning("Label reconciliation projection is unavailable after committed database outcome")
        print(dumps(result, sort_keys=True))
        return result
    finally:
        await db.disconnect()
