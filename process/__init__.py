import asyncio
import os

import click
import msgpack
import uvloop

from db.connection import init_db
from db.models import db
from process.label import download_label_content, init_label_file, label_shutdown, label_startup
from process.label import main as initiate_label_import
from process.label import process_label_results
from process.control_lifecycle import control_single_job_start
from process.drug_indications import import_drug_indications
from process.drug_indications import main as initiate_drug_indications_import
from process.ndc_product import download_content, init_file
from process.ndc_product import main as initiate_product_import
from process.ndc_product import process_results, shutdown, startup
from process.redis_config import redis_settings

uvloop.install()

NDC_QUEUE_NAME = (
    os.environ.get('HLTHPRT_ARQ_QUEUE_NDC')
    or os.environ.get('ARQ_QUEUE_NDC')
    or 'arq:queue:drug-api-import-ndc'
)
LABEL_QUEUE_NAME = (
    os.environ.get('HLTHPRT_ARQ_QUEUE_LABEL')
    or os.environ.get('ARQ_QUEUE_LABEL')
    or 'arq:queue:drug-api-import-label'
)


class NDC:
    functions = [init_file, download_content, process_results, control_single_job_start]
    on_startup = startup
    on_shutdown = shutdown
    queue_name = NDC_QUEUE_NAME
    redis_settings = redis_settings()
    job_serializer = msgpack.packb
    job_deserializer = lambda b: msgpack.unpackb(b, raw=False)


class Labeling:
    functions = [download_label_content, process_label_results, init_label_file, control_single_job_start]
    on_startup = label_startup
    on_shutdown = label_shutdown
    queue_name = LABEL_QUEUE_NAME
    queue_read_limit = 10
    redis_settings = redis_settings()
    job_serializer = msgpack.packb
    job_deserializer = lambda b: msgpack.unpackb(b, raw=False)


class DrugIndications:
    functions = [control_single_job_start]
    on_startup = lambda ctx: init_db(db, asyncio.get_event_loop())
    on_shutdown = lambda ctx: _close_db_bind()
    queue_name = 'arq:queue:drug-api-import-indications'
    redis_settings = redis_settings()
    job_serializer = msgpack.packb
    job_deserializer = lambda b: msgpack.unpackb(b, raw=False)


async def _close_db_bind():
    bind = db.pop_bind()
    if bind is not None:
        await bind.close()


@click.group()
def process_group():
    """
       Initiate run of importers
    """


@click.command(help="Run NDC Import")
def ndc():
    asyncio.run(initiate_product_import())


@click.command(help="Run Labeling Data Import")
def label():
    asyncio.run(initiate_label_import())


@click.command(help="Run drug-condition indication mapping import")
@click.option("--test", is_flag=True, help="Process a small label sample for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def drug_indications(test, import_id):
    asyncio.run(initiate_drug_indications_import(test_mode=test, import_id=import_id))


process_group.add_command(ndc)
process_group.add_command(label)
process_group.add_command(drug_indications, name='drug-indications')
