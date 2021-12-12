import datetime
import asyncio
import os
import tempfile
from pathlib import Path, PurePath
from arq import create_pool
from arq.connections import RedisSettings
from sqlalchemy.inspection import inspect
from orjson import loads as json_loads  # pylint: disable=maybe-no-member,no-name-in-module
from dateutil.parser import parse as parse_date
from aiofile import async_open
import ijson
from async_unzip.unzipper import unzip


from process.ext.utils import download_it, download_it_and_save, make_class, push_objects, print_time_info
from db.models import Label, db
from db.connection import init_db


async def download_label_content(ctx, task):
    # pylint: disable=too-many-locals
    redis = ctx['redis']
    print('Starting Labels file download: ', task.get('file'))

    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get('file'))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        await download_it_and_save(task.get('file'), tmp_filename)
        json_tmp_file = str(PurePath(str(tmpdirname), p.stem))

        await unzip(tmp_filename, tmpdirname)

        async with async_open(json_tmp_file, 'r') as afp:
            counter = 0
            send_counter = 0
            task = {'what': task.get('what'), 'model': 'label', 'results': []}

            async for res in ijson.items(afp, 'results.item'):
                task['results'].append(res)
                if counter == int(os.environ.get('SAVE_PER_PACK', 100)):
                    await redis.enqueue_job('process_label_results', task)
                    task['results'] = []
                    counter = -1
                    send_counter += 1
                counter += 1
            await redis.enqueue_job('process_label_results', task)
    print('Added taks: ', send_counter + 1)
    return 1


async def process_label_results(ctx, task):
    import_date = ctx['import_date']
    ctx['context']['run'] += 1
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
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.label_{import_date};")
    mylabel = make_class(Label, import_date)
    await mylabel.__table__.gino.create()


async def label_shutdown(ctx):
    import_date = ctx['import_date']
    if ctx['context'].get('label_count'):
        mylabel = make_class(Label, import_date)
        import_mylabel_count = await db.func.count(mylabel.id).gino.scalar()  # pylint: disable=E1101
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

                    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")

                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_product_ndc RENAME TO idx_product_ndc_old;")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_package_ndc RENAME TO idx_package_ndc_old;")
                    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")

                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_product_ndc_{import_date} RENAME TO idx_product_ndc;")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.idx_package_ndc_{import_date} RENAME TO idx_package_ndc;")
                    await db.status(f"ALTER TABLE IF EXISTS "
                                    f"{db_schema}.{table}_{import_date} RENAME TO {table};")

            print('Labeling rows in JSON:', ctx['context']['label_count'])
            print('Labling rows in DB: ', await db.func.count(Label.id).gino.scalar())  # pylint: disable=E1101
            print_time_info(ctx['context']['start'])
        else:
            print("Aborted: Imported rows Number differs from FDA rows number!")
    else:
        print('Labeling import failed')


async def init_label_file(ctx):
    redis = await create_pool(RedisSettings())
    r = await download_it(os.environ['MAIN_RX_JSON_URL'])
    obj = json_loads(r.content)
    ctx['context']['label_count'] = obj['results']['drug']['label']['total_records']
    print(f"Going to import {ctx['context']['label_count']} rows")
    for key in ['label']:
        for part in obj['results']['drug'][key]['partitions']:
            await redis.enqueue_job('download_label_content', {'what': key, 'file': part['file']})


async def main():
    redis = await create_pool(RedisSettings())
    await redis.enqueue_job('init_label_file')
