import asyncio
import datetime
import logging
import os
import tempfile
from json import loads
from pathlib import Path, PurePath

import ijson
import msgpack
from aiofile import async_open
from arq import create_pool
from async_unzip import unzip
from dateutil.parser import parse as parse_date
from sqlalchemy.inspection import inspect

from db.connection import init_db
from db.models import Label, db
from process.control_lifecycle import mark_control_run
from process.ext.utils import download_it, download_it_and_save, make_class, print_time_info, push_objects
from process.live_progress import enqueue_live_progress
from process.redis_config import redis_settings

logger = logging.getLogger(__name__)

LABEL_QUEUE_NAME = (
    os.environ.get('HLTHPRT_ARQ_QUEUE_LABEL')
    or os.environ.get('ARQ_QUEUE_LABEL')
    or 'arq:queue:drug-api-import-label'
)


async def download_label_content(ctx, task):
    # pylint: disable=too-many-locals
    redis = ctx['redis']
    max_records = int(task.get('max_records') or 0)
    run_id = task.get('run_id') or ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    partition_records = int(task.get('partition_records') or max_records or 0)
    partition_index = int(task.get('partition_index') or 1)
    partition_count = int(task.get('partition_count') or 1)
    print('Starting Labels file download: ', task.get('file'))
    enqueue_live_progress(
        run_id=run_id,
        importer="label",
        status="running",
        phase="label downloading partition",
        unit="partitions",
        done=max(partition_index - 1, 0),
        total=partition_count,
        message=f"downloading partition {partition_index}/{partition_count}",
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get('file'))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        await download_it_and_save(task.get('file'), tmp_filename)
        json_tmp_file = str(PurePath(str(tmpdirname), p.stem))

        await unzip(tmp_filename, tmpdirname)

        async with async_open(json_tmp_file, 'r') as afp:
            counter = 0
            read_counter = 0
            send_counter = 0
            batch_task = {
                'what': task.get('what'),
                'model': 'label',
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
                    await redis.enqueue_job('process_label_results', batch_task)
                    batch_task = {
                        'what': task.get('what'),
                        'model': 'label',
                        'results': [],
                        'run_id': run_id,
                        'partition_records': partition_records,
                    }
                    counter = -1
                    send_counter += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="label",
                        status="running",
                        phase="label parsing records",
                        unit="records",
                        done=read_counter,
                        total=partition_records or None,
                        message=f"parsed {read_counter} labels",
                    )
                counter += 1
            batch_task['batch_end'] = read_counter
            await redis.enqueue_job('process_label_results', batch_task)
            enqueue_live_progress(
                run_id=run_id,
                importer="label",
                status="running",
                phase="label partition parsed",
                unit="records",
                done=read_counter,
                total=partition_records or None,
                message=f"parsed {read_counter} labels",
            )
    print('Added taks: ', send_counter + 1)
    return 1


async def process_label_results(ctx, task):
    import_date = ctx['import_date']
    ctx['context']['run'] += 1
    run_id = task.get('run_id') or ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    mylabel = make_class(Label, import_date)

    obj_list = []

    product_columns = [column.name for column in inspect(mylabel).c]
    for res in task['results']:
        obj = {}
        for col in product_columns:
            if ("_date" in col) and res.get(col):
                obj[col] = parse_date(res.get(col), fuzzy=True)
            elif col == 'openfda':
                obj['product_ndc'] = res.get(col, {}).get('product_ndc', [])
                obj['package_ndc'] = res.get(col, {}).get('package_ndc', [])
            elif isinstance(res.get(col), list):
                obj[col] = ('\n'.join(res.get(col))).strip()
            elif res.get(col):
                obj[col] = res.get(col)
            elif col not in obj:
                obj[col] = None
        obj_list.append(obj)

    await push_objects(obj_list, mylabel)
    enqueue_live_progress(
        run_id=run_id,
        importer="label",
        status="running",
        phase="label saving records",
        unit="records",
        done=task.get('batch_end') or len(task.get('results') or []),
        total=task.get('partition_records') or None,
        message=f"saved {len(obj_list)} labels",
    )


async def label_startup(ctx):
    loop = asyncio.get_event_loop()
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.now()
    ctx['context']['run'] = 0
    ctx['context']['label_count'] = 0
    ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    await init_db(db, loop)
    import_date = ctx['import_date']
    db_schema = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') else 'rx_data'
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.label_{import_date};")
    mylabel = make_class(Label, import_date)
    await db.create_table(mylabel.__table__)


async def label_shutdown(ctx):
    try:
        await _label_shutdown_impl(ctx)
    except Exception as exc:
        control_run_id = ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
        try:
            await mark_control_run(
                control_run_id,
                status="failed",
                phase_detail="label import shutdown failed",
                progress_message="failed",
                error={"code": "shutdown_failed", "message": str(exc)},
            )
        except Exception as mark_exc:
            logger.warning("failed to mark label import shutdown failure: %s", mark_exc)
        raise


async def _label_shutdown_impl(ctx):
    import_date = ctx['import_date']
    control_run_id = ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    if ctx['context'].get('label_count'):
        mylabel = make_class(Label, import_date)
        import_mylabel_count = await db.select(db.func.count(mylabel.id)).scalar()
        if import_mylabel_count == ctx['context']['label_count']:
            db_schema = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') else 'rx_data'
            for table in ['label']:
                async with db.transaction():
                    print('Creating indexes..')
                    await db.status(
                        f"CREATE INDEX idx_product_ndc_{import_date} ON "
                        f"{db_schema}.{table}_{import_date} USING GIN(product_ndc);")
                    await db.status(
                        f"CREATE INDEX idx_package_ndc_{import_date} ON "
                        f"{db_schema}.{table}_{import_date} USING GIN(package_ndc);")
                    await db.status(
                        f"CREATE INDEX idx_label_id_{import_date} ON "
                        f"{db_schema}.{table}_{import_date} (id);")
                    await db.status(
                        f"CREATE INDEX idx_label_set_id_{import_date} ON "
                        f"{db_schema}.{table}_{import_date} (set_id);")

                    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")

                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_product_ndc RENAME TO idx_product_ndc_old;")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_package_ndc RENAME TO idx_package_ndc_old;")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_label_id RENAME TO idx_label_id_old;")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_label_set_id RENAME TO idx_label_set_id_old;")
                    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")

                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_product_ndc_{import_date} RENAME TO idx_product_ndc;")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_package_ndc_{import_date} RENAME TO idx_package_ndc;")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_label_id_{import_date} RENAME TO idx_label_id;")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_label_set_id_{import_date} RENAME TO idx_label_set_id;")
                    await db.status(f"ALTER TABLE IF EXISTS "
                                    f"{db_schema}.{table}_{import_date} RENAME TO {table};")

            print('Labeling rows in JSON:', ctx['context']['label_count'])
            print('Labling rows in DB: ', await db.select(db.func.count(Label.id)).scalar())
            print_time_info(ctx['context']['start'])
            await mark_control_run(
                control_run_id,
                status="succeeded",
                phase_detail="label import published",
                progress_message="succeeded",
                metrics={"source_label_count": ctx['context']['label_count'], "imported_label_count": import_mylabel_count},
                progress={
                    "unit": "records",
                    "total": ctx['context']['label_count'],
                    "done": import_mylabel_count,
                    "pct": 100,
                    "message": "succeeded",
                    "phase": "label import published",
                },
            )
        else:
            print(f"Aborted: Imported rows Number differs from FDA rows number! "
                  f"(JSON: {ctx['context']['label_count']}, DB: {import_mylabel_count})")
            await mark_control_run(
                control_run_id,
                status="failed",
                phase_detail="label import validation failed",
                progress_message="failed",
                error={"code": "validation_failed", "message": "imported label count does not match source count"},
            )
    else:
        print('Labeling import failed')
        await mark_control_run(
            control_run_id,
            status="failed",
            phase_detail="label import failed",
            progress_message="failed",
            error={"code": "import_failed", "message": "label_count was not set"},
        )


async def init_label_file(ctx, task=None):
    task = task if isinstance(task, dict) else {}
    if task.get('run_id'):
        ctx['control_run_id'] = task.get('run_id')
        ctx.setdefault('context', {})['control_run_id'] = task.get('run_id')
    test_mode = bool(task.get('test_mode') or task.get('test'))
    max_records = int(task.get('max_records') or os.environ.get('HLTHPRT_DRUG_IMPORT_TEST_MAX_RECORDS') or 5000)
    redis = await create_pool(redis_settings(),
                              default_queue_name=LABEL_QUEUE_NAME,
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))

    r = await download_it(os.environ['HLTHPRT_MAIN_RX_JSON_URL'])
    obj = loads(r.content)
    partitions = list(obj['results']['drug']['label']['partitions'])
    if test_mode:
        partitions = partitions[:1]
        ctx['context']['label_count'] = min(max_records, int(partitions[0].get('records') or max_records)) if partitions else 0
    else:
        ctx['context']['label_count'] = obj['results']['drug']['label']['total_records']
    print(f"Going to import {ctx['context']['label_count']} rows")
    control_run_id = ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id')
    await mark_control_run(
        control_run_id,
        status="running",
        phase_detail="label partitions enqueued",
        progress_message=f"queued {len(partitions)} partition(s)",
        metrics={"source_label_count": ctx['context']['label_count'], "partition_count": len(partitions)},
        progress={
            "unit": "records",
            "total": ctx['context']['label_count'],
            "done": 0,
            "pct": 0,
            "message": f"queued {len(partitions)} partition(s)",
        },
    )
    for partition_index, part in enumerate(partitions, start=1):
        payload = {
            'what': 'label',
            'file': part['file'],
            'run_id': control_run_id,
            'partition_records': min(max_records, int(part.get('records') or max_records)) if test_mode else int(part.get('records') or 0),
            'partition_index': partition_index,
            'partition_count': len(partitions),
        }
        if test_mode:
            payload['max_records'] = max_records
        await redis.enqueue_job('download_label_content', payload)


async def main():
    redis = await create_pool(redis_settings(),
                              default_queue_name=LABEL_QUEUE_NAME,
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    await redis.enqueue_job('init_label_file')
