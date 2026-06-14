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


async def download_content(ctx, task):
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
        p = Path(task.get('file'))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        json_tmp_file = str(PurePath(str(tmpdirname), p.stem))

        await download_it_and_save(task.get('file'), tmp_filename)

        await unzip(tmp_filename, tmpdirname)

        async with async_open(json_tmp_file, 'r') as afp:
            counter = 0
            read_counter = 0
            send_counter = 0
            batch_task = {
                'what': task.get('what'),
                'model': 'product',
                'results': [],
                'run_id': run_id,
                'partition_records': partition_records,
            }

            async for res in ijson.items(afp, 'results.item'):
                batch_task['results'].append(res)
                read_counter += 1
                if max_records and read_counter >= max_records:
                    break
                if counter == int(os.environ.get('SAVE_PER_PACK', 100)):
                    batch_task['batch_end'] = read_counter
                    await redis.enqueue_job('process_results', batch_task)
                    batch_task = {
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
            batch_task['batch_end'] = read_counter
            await redis.enqueue_job('process_results', batch_task)
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
    import_date = ctx['import_date']
    ctx['context']['run'] += 1
    run_id = task.get('run_id') or ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    myproduct = make_class(Product, import_date)
    mypackage = make_class(Package, import_date)

    obj_list = []
    packagin_obj_list = []

    for res in task['results']:
        obj = {}
        for col in [column.name for column in inspect(myproduct).c]:
            if ("_date" in col) and res.get(col):
                obj[col] = parse_date(res.get(col), fuzzy=True)
            elif res.get(col):
                obj[col] = res.get(col)
            elif col not in obj:
                obj[col] = None
        openfda_payload = res.get('openfda') or {}
        rxnorm_values = openfda_payload.get('rxcui') or []
        obj['rxnorm_ids'] = [str(value) for value in rxnorm_values]
        obj['is_otc'] = _derive_is_otc(res)

        if not ('dosage_form' in obj) and obj['dosage_form']:
            obj['dosage_form'] = ''
        obj['short_dosage_form'] = obj['dosage_form'].split(',')[0]

        for pkg in res['packaging']:
            packagin_obj = {}
            for pkg_col in [column.name for column in inspect(Package).c]:
                if ("_date" in pkg_col) and pkg.get(pkg_col):
                    packagin_obj[pkg_col] = parse_date(pkg.get(pkg_col), fuzzy=True)
                elif pkg_col in ['product_ndc', 'package_ndc']:
                    pkg['product_ndc'] = obj['product_ndc']
                    packagin_obj[pkg_col] = pkg.get(pkg_col)
                elif pkg.get(pkg_col):
                    packagin_obj[pkg_col] = pkg.get(pkg_col)
                elif pkg_col not in packagin_obj:
                    packagin_obj[pkg_col] = None
            packagin_obj['product_ndc'] = obj['product_ndc']
            if not ('description' in packagin_obj) and packagin_obj['description']:
                packagin_obj['description'] = ''

            if len(packagin_obj['package_ndc']) < 12:
                packagin_obj['package_ndc'] = '-'.join((packagin_obj['product_ndc'], packagin_obj['package_ndc']))

            tmp_arr = packagin_obj['package_ndc'].split('-')

            if (len(''.join(tmp_arr)) != 11):
                if(len(tmp_arr[0]) == 4):
                    tmp_arr[0] = '0' + tmp_arr[0]
                elif(len(tmp_arr[1]) == 3):
                    tmp_arr[1] = '0' + tmp_arr[1]
                elif(len(tmp_arr[2]) == 1):
                    tmp_arr[2] = '0' + tmp_arr[2]
            packagin_obj['ndc11'] = ''.join(tmp_arr)

            if (tmp_arr_match := product_description_re.match(packagin_obj['description'])):
                (packagin_obj['size'], packagin_obj['size_extra'], packagin_obj['packages_number'],
                packagin_obj['package_format']) = tmp_arr_match.groups()
                packagin_obj['size'] = int(packagin_obj['size'])
                packagin_obj['packages_number'] = int(packagin_obj['packages_number'])
                if packagin_obj['size_extra'] == obj['dosage_form']:
                    packagin_obj['size_extra'] = ''

            packagin_obj_list.append(packagin_obj)
        obj_list.append(obj)

    unique_packages = {}
    for item in packagin_obj_list:
        package_ndc = item.get('package_ndc')
        if package_ndc:
            unique_packages[package_ndc] = item
    unique_products = {}
    for item in obj_list:
        product_id = item.get('product_id')
        if product_id:
            unique_products[product_id] = item

    await push_objects(list(unique_packages.values()), mypackage)
    await push_objects(list(unique_products.values()), myproduct)
    enqueue_live_progress(
        run_id=run_id,
        importer="ndc",
        status="running",
        phase="ndc saving records",
        unit="records",
        done=task.get('batch_end') or len(task.get('results') or []),
        total=task.get('partition_records') or None,
        message=f"saved {len(unique_products)} products",
    )


async def startup(ctx):
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
    import_date = ctx['import_date']
    control_run_id = ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    if ctx['context'].get('product_count'):
        myproduct = make_class(Product, import_date)
        import_product_count = await db.select(db.func.count(myproduct.product_id)).scalar()
        expected_product_count = int(ctx['context']['product_count'])
        minimum_expected = int(expected_product_count * 0.95)
        if import_product_count and import_product_count >= minimum_expected:
            db_schema = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') else 'rx_data'
            await db.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            await db.status("CREATE EXTENSION IF NOT EXISTS btree_gin;")
            for table in ['product', 'package']:
                async with db.transaction():
                    print(f'Creating indexes for {table} ...')
                    await db.status(
                        f"CREATE INDEX {table}_idx_product_ndc_{import_date} ON "
                        f"{db_schema}.{table}_{import_date} USING GIN(product_ndc);")

                    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")

                    if table == 'product':
                        await db.status(f"CREATE INDEX {table}_idx_brand_trgm_idx_{import_date} ON "
                                        f"{db_schema}.{table}_{import_date} "
                                        f"USING GIN(brand_name gin_trgm_ops);")
                        await db.status(f"CREATE INDEX {table}_idx_generic_trgm_idx_{import_date} ON "
                                        f"{db_schema}.{table}_{import_date} USING "
                                        f"GIN(generic_name gin_trgm_ops);")
                        await db.status(
                            f"CREATE INDEX product_rxnorm_idx_{import_date} ON "
                            f"{db_schema}.product_{import_date} USING GIN(rxnorm_ids);"
                        )

                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.{table}_idx_product_ndc RENAME TO "
                                    f"{table}_idx_product_ndc_old;")

                    if table == 'product':
                        await db.status(f"ALTER INDEX IF EXISTS "
                                        f"{db_schema}.{table}_idx_brand_trgm_idx RENAME TO "
                                        f"{table}_idx_brand_trgm_idx_old;")
                        await db.status(f"ALTER INDEX IF EXISTS "
                                        f"{db_schema}.{table}_idx_generic_trgm_idx RENAME TO "
                                        f"{table}_idx_generic_trgm_idx_old;")
                        await db.status(f"ALTER INDEX IF EXISTS "
                                        f"{db_schema}.product_rxnorm_idx RENAME TO "
                                        f"product_rxnorm_idx_old;")

                        await db.status(f"ALTER INDEX IF EXISTS "
                                        f"{db_schema}.{table}_idx_brand_trgm_idx_{import_date} RENAME TO "
                                        f"{table}_idx_brand_trgm_idx;")
                        await db.status(f"ALTER INDEX IF EXISTS "
                                        f"{db_schema}.{table}_idx_generic_trgm_idx_{import_date} RENAME TO "
                                        f"{table}_idx_generic_trgm_idx;")
                        await db.status(f"ALTER INDEX IF EXISTS "
                                        f"{db_schema}.product_rxnorm_idx_{import_date} RENAME TO "
                                        f"product_rxnorm_idx;")

                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.{table}_idx_product_ndc_{import_date} RENAME TO "
                                    f"{table}_idx_product_ndc;")

                    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
                    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table}_{import_date} RENAME TO {table};")

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
        else:
            print(f"Aborted: Imported rows Number differs from FDA rows number! "
                  f"(JSON: {expected_product_count}, DB: {import_product_count})")
            await mark_control_run(
                control_run_id,
                status="failed",
                phase_detail="ndc import validation failed",
                progress_message="failed",
                error={"code": "validation_failed", "message": "imported product count below expected threshold"},
            )
    else:
        print('Product import failed')
        await mark_control_run(
            control_run_id,
            status="failed",
            phase_detail="ndc import failed",
            progress_message="failed",
            error={"code": "import_failed", "message": "product_count was not set"},
        )


async def init_file(ctx, task=None):
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
    r = await download_it(os.environ['HLTHPRT_MAIN_RX_JSON_URL'])
    # it is very small in this case
    obj = loads(r.content)
    partitions = list(obj['results']['drug']['ndc']['partitions'])
    if test_mode:
        partitions = partitions[:1]
        ctx['context']['product_count'] = min(max_records, int(partitions[0].get('records') or max_records)) if partitions else 0
    else:
        ctx['context']['product_count'] = obj['results']['drug']['ndc']['total_records']
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
        payload = {
            'what': 'ndc',
            'file': part['file'],
            'run_id': control_run_id,
            'partition_records': min(max_records, int(part.get('records') or max_records)) if test_mode else int(part.get('records') or 0),
            'partition_index': partition_index,
            'partition_count': len(partitions),
        }
        if test_mode:
            payload['max_records'] = max_records
        await redis.enqueue_job('download_content', payload)


async def main():
    redis = await create_pool(redis_settings(),
                              default_queue_name=NDC_QUEUE_NAME,
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    x = await redis.enqueue_job('init_file')
