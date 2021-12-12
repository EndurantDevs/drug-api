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
from async_unzip.unzipper import unzip


from process.ext.utils import download_it, download_it_and_save, make_class, push_objects, print_time_info
from db.models import Product, Package, db
from db.connection import init_db


async def download_content(ctx, task):
    redis = ctx['redis']
    print('Starting NDC data download: ', task.get('file'))
    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get('file'))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        json_tmp_file = str(PurePath(str(tmpdirname), p.stem))

        await download_it_and_save(task.get('file'), tmp_filename)

        await unzip(tmp_filename, tmpdirname)

        async with async_open(json_tmp_file, 'r') as afp:
            obj = json_loads(await afp.read())  # pylint: disable=maybe-no-member

            counter = 0
            send_counter = 0
            task = {'what': task.get('what'), 'model': 'product', 'results': []}
            ctx['context']['product_count'] = len(obj['results'])

            for res in obj['results']:
                task['results'].append(res)
                if counter == int(os.environ.get('SAVE_PER_PACK', 100)):
                    await redis.enqueue_job('process_results', task)
                    task['results'] = []
                    counter = -1
                    send_counter += 1
                counter += 1
            await redis.enqueue_job('process_results', task)
    print('Added taks: ', send_counter + 1)
    return 1


async def process_results(ctx, task):
    import_date = ctx['import_date']
    ctx['context']['run'] += 1
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
            packagin_obj_list.append(packagin_obj)
        obj_list.append(obj)

    await push_objects(packagin_obj_list, mypackage)
    await push_objects(obj_list, myproduct)


async def startup(ctx):
    loop = asyncio.get_event_loop()
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.now()
    ctx['context']['run'] = 0
    ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    await init_db(db, loop)
    import_date = ctx['import_date']
    db_schema = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') else 'rx_data'
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.product_{import_date};")
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.package_{import_date};")
    myproduct = make_class(Product, import_date)
    mypackage = make_class(Package, import_date)
    await myproduct.__table__.gino.create()
    await mypackage.__table__.gino.create()


async def shutdown(ctx):
    import_date = ctx['import_date']
    if ctx['context'].get('product_count'):
        myproduct = make_class(Product, import_date)
        import_product_count = await db.func.count(myproduct.product_id).gino.scalar()  # pylint: disable=E1101
        if import_product_count == ctx['context']['product_count']:
            db_schema = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') else 'rx_data'
            for table in ['product', 'package']:
                async with db.transaction():
                    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
                    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
                    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table}_{import_date} RENAME TO {table};")

            print('Products in JSON:', ctx['context']['product_count'])
            print('Products in DB: ', await db.func.count(Product.product_id).gino.scalar())  # pylint: disable=E1101
            print('Packages in DB: ', await db.func.count(Package.package_ndc).gino.scalar())  # pylint: disable=E1101
            print_time_info(ctx['context']['start'])
        else:
            print("Aborted: Imported rows Number differs from FDA rows number!")
    else:
        print('Product import failed')


async def init_file(ctx):
    redis = await create_pool(RedisSettings())
    r = await download_it(os.environ['MAIN_RX_JSON_URL'])
    obj = json_loads(r.content)

    for key in ['ndc']:
        for part in obj['results']['drug'][key]['partitions']:
            await redis.enqueue_job('download_content', {'what': key, 'file': part['file']})


async def main():
    redis = await create_pool(RedisSettings())
    await redis.enqueue_job('init_file')
