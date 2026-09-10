import asyncio
import logging
import os
import re
import uuid
from typing import Optional

import msgpack
from arq import create_pool
from dateutil.parser import parse as parse_date

from db.connection import init_db
from db.models import Package, Product, db
from process.control_lifecycle import ensure_import_run_table
from process.import_status_events import enqueue_status_event
from process.live_progress import enqueue_live_progress
from process.ndc_acquire import acquire_ndc_manifest, consume_ndc_partitions
from process.ndc_publish import publish_ndc_tables
from process.ndc_stage import create_ndc_stages, discard_ndc_stages, fail_ndc_attempt, new_ndc_attempt, save_ndc_batch
from process.redis_config import redis_settings

logger = logging.getLogger(__name__)

product_description_re = re.compile(r'(\d+) (.*?) in (\d+) (.*?) \(\d+-\d+-\d+\)')
NDC_QUEUE_NAME = (
    os.environ.get('HLTHPRT_ARQ_QUEUE_NDC')
    or os.environ.get('ARQ_QUEUE_NDC')
    or 'arq:queue:drug-api-import-ndc'
)



def _derive_is_otc(res: dict) -> Optional[bool]:
    marketing_category = str(res.get('marketing_category') or '').strip().lower()
    product_type = str(res.get('product_type') or '').strip().lower()
    openfda_payload = res.get('openfda') or {}
    openfda_product_type = " ".join(str(item) for item in (openfda_payload.get('product_type') or [])).strip().lower()
    signal = " ".join(part for part in (marketing_category, product_type, openfda_product_type) if part)

    if not signal:
        return None
    if 'otc' in signal or 'over-the-counter' in signal or 'over the counter' in signal:
        return True
    if 'prescription' in signal or signal.startswith('rx ') or ' rx ' in f' {signal} ':
        return False
    return None


def _record_column_value(record_dict: dict, column_name: str) -> object:
    raw_value = record_dict.get(column_name)
    if ("_date" in column_name) and raw_value:
        return parse_date(raw_value, fuzzy=True).date()
    if raw_value:
        return raw_value
    return None


def _product_row_dict_from_record(product_record: dict, product_columns: list[str]) -> dict[str, object]:
    product_row_dict = {
        product_column: _record_column_value(product_record, product_column)
        for product_column in product_columns
    }
    openfda_dict = product_record.get('openfda') or {}
    rxnorm_values = openfda_dict.get('rxcui') or []
    product_row_dict['rxnorm_ids'] = [str(rxnorm_value) for rxnorm_value in rxnorm_values]
    product_row_dict['is_otc'] = _derive_is_otc(product_record)

    product_row_dict['dosage_form'] = product_row_dict['dosage_form'] or ''
    product_row_dict['short_dosage_form'] = product_row_dict['dosage_form'].split(',')[0]
    return product_row_dict


def _package_column_value(
    package_record: dict,
    product_row_dict: dict,
    package_column: str,
) -> object:
    if ("_date" in package_column) and package_record.get(package_column):
        return parse_date(package_record.get(package_column), fuzzy=True).date()
    if package_column in ['product_ndc', 'package_ndc']:
        package_record['product_ndc'] = product_row_dict['product_ndc']
        return package_record.get(package_column)
    if package_record.get(package_column):
        return package_record.get(package_column)
    return None


def _normalize_package_ndc(package_row_dict: dict) -> None:
    if len(package_row_dict['package_ndc']) < 12:
        package_row_dict['package_ndc'] = '-'.join(
            (package_row_dict['product_ndc'], package_row_dict['package_ndc'])
        )

    ndc_segments = package_row_dict['package_ndc'].split('-')
    if len(''.join(ndc_segments)) != 11:
        if len(ndc_segments[0]) == 4:
            ndc_segments[0] = '0' + ndc_segments[0]
        elif len(ndc_segments[1]) == 3:
            ndc_segments[1] = '0' + ndc_segments[1]
        elif len(ndc_segments[2]) == 1:
            ndc_segments[2] = '0' + ndc_segments[2]
    package_row_dict['ndc11'] = ''.join(ndc_segments)


def _apply_package_description(package_row_dict: dict, product_row_dict: dict) -> None:
    description_match = product_description_re.match(package_row_dict['description'])
    if not description_match:
        return
    (
        package_row_dict['size'],
        package_row_dict['size_extra'],
        package_row_dict['packages_number'],
        package_row_dict['package_format'],
    ) = description_match.groups()
    package_row_dict['size'] = int(package_row_dict['size'])
    package_row_dict['packages_number'] = int(package_row_dict['packages_number'])
    if package_row_dict['size_extra'] == product_row_dict['dosage_form']:
        package_row_dict['size_extra'] = ''


def _package_row_dict_from_record(
    package_record: dict,
    product_row_dict: dict,
    package_columns: list[str],
) -> dict[str, object]:
    package_row_dict = {
        package_column: _package_column_value(package_record, product_row_dict, package_column)
        for package_column in package_columns
    }
    package_row_dict['product_ndc'] = product_row_dict['product_ndc']
    package_row_dict['description'] = package_row_dict['description'] or ''

    _normalize_package_ndc(package_row_dict)
    _apply_package_description(package_row_dict, product_row_dict)
    return package_row_dict


def _package_rows_from_record(source_record: dict, product: dict, package_columns: list[str]) -> list[dict]:
    """Keep every package occurrence, representing contradictory start dates as unknown."""
    rows = [_package_row_dict_from_record(package, product, package_columns)
            for package in source_record['packaging']]
    dates_by_package = {}
    for row in rows:
        dates_by_package.setdefault(row['package_ndc'], set()).add(row['marketing_start_date'])
    ambiguous = {key for key, dates in dates_by_package.items() if len(dates) > 1}
    if ambiguous:
        logger.warning("NDC package start dates disagree within one product; using unknown dates for %d identities",
                       len(ambiguous))
    for row in rows:
        if row['package_ndc'] in ambiguous:
            row['marketing_start_date'] = None
    return rows


async def process_results(ctx, task):
    """Normalize and acknowledge one complete batch against its coordinator's stages."""
    attempt = ctx['ndc_attempt']
    product_columns = [column.name for column in Product.__table__.columns]
    package_columns = [column.name for column in Package.__table__.columns]
    products = []
    packages = []
    for source_record in task['results']:
        product = _product_row_dict_from_record(source_record, product_columns)
        packages.extend(_package_rows_from_record(source_record, product, package_columns))
        products.append(product)
    await save_ndc_batch(db, attempt, products, packages)
    enqueue_live_progress(
        run_id=attempt.run_id, importer="ndc", status="running", phase="ndc saving records",
        unit="records", done=attempt.counts["source_products"],
        message=f"saved {attempt.counts['product']} unique products and {attempt.counts['package']} packages",
    )


async def startup(ctx):
    """Connect the worker without adopting or dropping another attempt's stages."""
    await init_db(db, asyncio.get_running_loop())


async def shutdown(ctx):
    """Close the worker connection pool; completion belongs to the coordinator job."""
    await db.disconnect()


async def init_file(ctx, task=None):
    """Await complete NDC acquisition, paired saves, and atomic native publication."""
    task = task if isinstance(task, dict) else {}
    standalone_id = uuid.uuid5(uuid.NAMESPACE_URL, str(ctx["job_id"])) if ctx.get("job_id") else uuid.uuid4()
    run_id = task.get("run_id") or "ndc-" + standalone_id.hex
    attempt = new_ndc_attempt(run_id, os.getenv("DB_SCHEMA") or "rx_data")
    # Each partition inherits only this attempt, never a previous worker's context.
    local_context_dict = {"ndc_attempt": attempt}

    async def consume_batch(records):
        """Acknowledge only a complete paired write for this attempt."""
        await process_results(local_context_dict, {"results": records})

    try:
        async with asyncio.timeout(120):
            await ensure_import_run_table()
            await create_ndc_stages(db, attempt, standalone=not bool(task.get("run_id")))
        is_sample = bool(task.get("test_mode") or task.get("test"))
        max_records = int(task.get("max_records") or os.getenv("HLTHPRT_DRUG_IMPORT_TEST_MAX_RECORDS") or 5000)
        if max_records <= 0:
            raise ValueError("NDC sample record limit must be positive")
        manifest = await acquire_ndc_manifest(
            os.environ["HLTHPRT_MAIN_RX_JSON_URL"], sample=is_sample, max_records=max_records,
        )
        acquisition = await consume_ndc_partitions(manifest, consume_batch)
        async with asyncio.timeout(1800):
            receipt = await publish_ndc_tables(db, attempt.schema, attempt.suffix,
                                               attempt=attempt, acquisition=acquisition)
    except BaseException:
        try:
            async with asyncio.timeout(10):
                if await fail_ndc_attempt(db, attempt) == 1:
                    _announce_ndc_failure(attempt)
        except Exception:
            logger.warning("NDC failure status could not be recorded")
        try:
            async with asyncio.timeout(10):
                await discard_ndc_stages(db, attempt)
        except Exception:
            logger.warning("NDC owned-stage cleanup failed; retained for inspection")
        raise
    _announce_ndc_completion(attempt, receipt)
    return receipt


def _announce_ndc_completion(attempt, receipt):
    phase = "ndc import published" if receipt["complete"] else "ndc sample validated"
    try:
        enqueue_status_event({
            "run_id": attempt.run_id, "importer": "ndc", "status": "succeeded", "phase_detail": phase,
            "finished_at": receipt.get("completed_at"),
            "metrics": {"ndc_attempt_id": attempt.suffix,
                        "ndc_publication" if receipt["complete"] else "ndc_sample": receipt,
                        "source_product_count": attempt.counts["source_products"],
                        "imported_product_count": attempt.counts["product"]},
            "progress": {"unit": "records", "done": attempt.counts["source_products"],
                         "total": attempt.counts["source_products"], "pct": 100, "message": "succeeded"},
        })
        enqueue_live_progress(
            run_id=attempt.run_id, importer="ndc", status="succeeded", unit="records",
            done=attempt.counts["source_products"], total=attempt.counts["source_products"], pct=100,
            phase=phase,
            message="succeeded",
        )
    except Exception:
        logger.warning("NDC completion event unavailable; native success is already committed")


async def main():
    """Enqueue the default NDC import manifest task."""
    redis = await create_pool(redis_settings(),
                              default_queue_name=NDC_QUEUE_NAME,
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    await redis.enqueue_job('init_file')


def _announce_ndc_failure(attempt):
    try:
        enqueue_status_event({
            "run_id": attempt.run_id, "importer": "ndc", "status": "failed",
            "phase_detail": "ndc import failed", "metrics": {"ndc_attempt_id": attempt.suffix},
            "error": {"code": "ndc_import_failed"},
        })
    except Exception:
        logger.warning("NDC failure event could not be queued; native failure retained")
