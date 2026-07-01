import asyncio
import datetime
import logging
import os
import re
import tempfile
from json import loads
from pathlib import Path, PurePath
from typing import Optional

import ijson
import msgpack
from aiofile import async_open
from arq import create_pool
from async_unzip import unzip
from dateutil.parser import parse as parse_date
from sqlalchemy.inspection import inspect

from db.connection import init_db
from db.models import Package, Product, db
from process.control_lifecycle import mark_control_run
from process.ext.utils import download_it, download_it_and_save, make_class, print_time_info, push_objects
from process.live_progress import enqueue_live_progress
from process.ndc_publish import publish_ndc_tables
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
        return parse_date(raw_value, fuzzy=True)
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

    if not ('dosage_form' in product_row_dict) and product_row_dict['dosage_form']:
        product_row_dict['dosage_form'] = ''
    product_row_dict['short_dosage_form'] = product_row_dict['dosage_form'].split(',')[0]
    return product_row_dict


def _package_column_value(
    package_record: dict,
    product_row_dict: dict,
    package_column: str,
) -> object:
    if ("_date" in package_column) and package_record.get(package_column):
        return parse_date(package_record.get(package_column), fuzzy=True)
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
    if not ('description' in package_row_dict) and package_row_dict['description']:
        package_row_dict['description'] = ''

    _normalize_package_ndc(package_row_dict)
    _apply_package_description(package_row_dict, product_row_dict)
    return package_row_dict


async def download_content(ctx, task):
    """Download one FDA NDC partition and enqueue product parse batches."""
    redis = ctx['redis']
    max_records = int(task.get('max_records') or 0)
    run_id = task.get('run_id') or ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    partition_records = int(task.get('partition_records') or max_records or 0)
    partition_index = int(task.get('partition_index') or 1)
    partition_count = int(task.get('partition_count') or 1)
    print('Starting NDC data download: ', task.get('file'))
    enqueue_live_progress(
        run_id=run_id,
        importer="ndc",
        status="running",
        phase="ndc downloading partition",
        unit="partitions",
        done=max(partition_index - 1, 0),
        total=partition_count,
        message=f"downloading partition {partition_index}/{partition_count}",
    )
    with tempfile.TemporaryDirectory() as tmpdirname:
        archive_path = Path(task.get('file'))
        tmp_filename = str(PurePath(str(tmpdirname), archive_path.name))
        json_tmp_file = str(PurePath(str(tmpdirname), archive_path.stem))

        await download_it_and_save(task.get('file'), tmp_filename)

        await unzip(tmp_filename, tmpdirname)

        async with async_open(json_tmp_file, 'r') as afp:
            counter = 0
            read_counter = 0
            send_counter = 0
            batch_task_dict = {
                'what': task.get('what'),
                'model': 'product',
                'results': [],
                'run_id': run_id,
                'partition_records': partition_records,
            }

            async for product_record in ijson.items(afp, 'results.item'):
                batch_task_dict['results'].append(product_record)
                read_counter += 1
                if max_records and read_counter >= max_records:
                    break
                if counter == int(os.environ.get('SAVE_PER_PACK', 100)):
                    batch_task_dict['batch_end'] = read_counter
                    await redis.enqueue_job('process_results', batch_task_dict)
                    batch_task_dict = {
                        'what': task.get('what'),
                        'model': 'product',
                        'results': [],
                        'run_id': run_id,
                        'partition_records': partition_records,
                    }
                    counter = -1
                    send_counter += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="ndc",
                        status="running",
                        phase="ndc parsing records",
                        unit="records",
                        done=read_counter,
                        total=partition_records or None,
                        message=f"parsed {read_counter} records",
                    )
                counter += 1
            batch_task_dict['batch_end'] = read_counter
            await redis.enqueue_job('process_results', batch_task_dict)
            enqueue_live_progress(
                run_id=run_id,
                importer="ndc",
                status="running",
                phase="ndc partition parsed",
                unit="records",
                done=read_counter,
                total=partition_records or None,
                message=f"parsed {read_counter} records",
            )

    print('Added taks: ', send_counter + 1)
    return 1


async def process_results(ctx, task):
    """Normalize FDA NDC records into product and package import rows."""
    import_date = ctx['import_date']
    ctx['context']['run'] += 1
    run_id = task.get('run_id') or ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    myproduct = make_class(Product, import_date)
    mypackage = make_class(Package, import_date)

    product_columns = [column.name for column in inspect(myproduct).c]
    package_columns = [column.name for column in inspect(Package).c]

    product_rows = []
    package_rows = []
    for product_record in task['results']:
        product_row_dict = _product_row_dict_from_record(product_record, product_columns)
        for package_record in product_record['packaging']:
            package_rows.append(_package_row_dict_from_record(package_record, product_row_dict, package_columns))
        product_rows.append(product_row_dict)

    package_by_ndc = {}
    for package_row_dict in package_rows:
        package_ndc = package_row_dict.get('package_ndc')
        if package_ndc:
            package_by_ndc[package_ndc] = package_row_dict
    product_by_id = {}
    for product_row_dict in product_rows:
        product_id = product_row_dict.get('product_id')
        if product_id:
            product_by_id[product_id] = product_row_dict

    await push_objects(list(package_by_ndc.values()), mypackage)
    await push_objects(list(product_by_id.values()), myproduct)
    enqueue_live_progress(
        run_id=run_id,
        importer="ndc",
        status="running",
        phase="ndc saving records",
        unit="records",
        done=task.get('batch_end') or len(task.get('results') or []),
        total=task.get('partition_records') or None,
        message=f"saved {len(product_by_id)} products",
    )


async def startup(ctx):
    """Prepare dated product and package import tables before workers start."""
    loop = asyncio.get_event_loop()
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.now()
    ctx['context']['run'] = 0
    ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    await init_db(db, loop)
    import_date = ctx['import_date']
    db_schema = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') else 'rx_data'
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.product_{import_date};")
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.package_{import_date};")
    myproduct = make_class(Product, import_date)
    mypackage = make_class(Package, import_date)
    await db.create_table(myproduct.__table__)
    await db.create_table(mypackage.__table__)
    print("Preparing done")


async def shutdown(ctx):
    """Publish or fail the NDC import and mark the control run."""
    try:
        await _shutdown_impl(ctx)
    except Exception as exc:
        control_run_id = ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
        try:
            await mark_control_run(
                control_run_id,
                status="failed",
                phase_detail="ndc import shutdown failed",
                progress_message="failed",
                error={"code": "shutdown_failed", "message": str(exc)},
            )
        except Exception as mark_exc:
            logger.warning("failed to mark ndc import shutdown failure: %s", mark_exc)
        raise


async def _shutdown_impl(ctx):
    """Validate product counts, swap NDC tables, and report final status."""
    import_date = ctx['import_date']
    control_run_id = ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    if not ctx['context'].get('product_count'):
        print('Product import failed')
        await mark_control_run(
            control_run_id,
            status="failed",
            phase_detail="ndc import failed",
            progress_message="failed",
            error={"code": "import_failed", "message": "product_count was not set"},
        )
        return

    myproduct = make_class(Product, import_date)
    import_product_count = await db.select(db.func.count(myproduct.product_id)).scalar()
    expected_product_count = int(ctx['context']['product_count'])
    minimum_expected = int(expected_product_count * 0.95)
    if not import_product_count or import_product_count < minimum_expected:
        print(f"Aborted: Imported rows Number differs from FDA rows number! "
              f"(JSON: {expected_product_count}, DB: {import_product_count})")
        await mark_control_run(
            control_run_id,
            status="failed",
            phase_detail="ndc import validation failed",
            progress_message="failed",
            error={"code": "validation_failed", "message": "imported product count below expected threshold"},
        )
        return

    db_schema = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') else 'rx_data'
    await publish_ndc_tables(db, db_schema, import_date)
    await _mark_ndc_success(ctx, control_run_id, expected_product_count, import_product_count)


async def _mark_ndc_success(
    ctx: dict,
    control_run_id: str | None,
    expected_product_count: int,
    import_product_count: int,
) -> None:
    print('Products in JSON:', expected_product_count)
    print('Products in DB: ', await db.select(db.func.count(Product.product_id)).scalar())
    print('Packages in DB: ', await db.select(db.func.count(Package.package_ndc)).scalar())
    if import_product_count != expected_product_count:
        print(
            f"WARNING: Source total_records ({expected_product_count}) "
            f"does not exactly match imported unique product rows ({import_product_count})."
        )
    print_time_info(ctx['context']['start'])
    await mark_control_run(
        control_run_id,
        status="succeeded",
        phase_detail="ndc import published",
        progress_message="succeeded",
        metrics={"source_product_count": expected_product_count, "imported_product_count": import_product_count},
        progress={
            "unit": "records",
            "total": expected_product_count,
            "done": import_product_count,
            "pct": 100,
            "message": "succeeded",
            "phase": "ndc import published",
        },
    )


async def init_file(ctx, task=None):
    """Load the FDA NDC manifest and enqueue one task per selected partition."""
    task = task if isinstance(task, dict) else {}
    if task.get('run_id'):
        ctx['control_run_id'] = task.get('run_id')
        ctx.setdefault('context', {})['control_run_id'] = task.get('run_id')
    test_mode = bool(task.get('test_mode') or task.get('test'))
    max_records = int(task.get('max_records') or os.environ.get('HLTHPRT_DRUG_IMPORT_TEST_MAX_RECORDS') or 5000)
    redis = await create_pool(redis_settings(),
                              default_queue_name=NDC_QUEUE_NAME,
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    print('Downloading data from: ', os.environ['HLTHPRT_MAIN_RX_JSON_URL'])
    response_content = await download_it(os.environ['HLTHPRT_MAIN_RX_JSON_URL'])
    # it is very small in this case
    manifest_dict = loads(response_content.content)
    partitions = list(manifest_dict['results']['drug']['ndc']['partitions'])
    if test_mode:
        partitions = partitions[:1]
        ctx['context']['product_count'] = min(max_records, int(partitions[0].get('records') or max_records)) if partitions else 0
    else:
        ctx['context']['product_count'] = manifest_dict['results']['drug']['ndc']['total_records']
    print(f"Going to import {ctx['context']['product_count']} rows")
    control_run_id = ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    await mark_control_run(
        control_run_id,
        status="running",
        phase_detail="ndc partitions enqueued",
        progress_message=f"queued {len(partitions)} partition(s)",
        metrics={"source_product_count": ctx['context']['product_count'], "partition_count": len(partitions)},
        progress={
            "unit": "records",
            "total": ctx['context']['product_count'],
            "done": 0,
            "pct": 0,
            "message": f"queued {len(partitions)} partition(s)",
        },
    )
    for partition_index, part in enumerate(partitions, start=1):
        partition_task_dict = {
            'what': 'ndc',
            'file': part['file'],
            'run_id': control_run_id,
            'partition_records': min(max_records, int(part.get('records') or max_records)) if test_mode else int(part.get('records') or 0),
            'partition_index': partition_index,
            'partition_count': len(partitions),
        }
        if test_mode:
            partition_task_dict['max_records'] = max_records
        await redis.enqueue_job('download_content', partition_task_dict)


async def main():
    """Enqueue the default NDC import manifest task."""
    redis = await create_pool(redis_settings(),
                              default_queue_name=NDC_QUEUE_NAME,
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    await redis.enqueue_job('init_file')
