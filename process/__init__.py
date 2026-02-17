import asyncio
import os

import click
import msgpack
import uvloop
from arq.connections import RedisSettings

from process.label import download_label_content, init_label_file, label_shutdown, label_startup
from process.label import main as initiate_label_import
from process.label import process_label_results
from process.ndc_product import download_content, init_file
from process.ndc_product import main as initiate_product_import
from process.ndc_product import process_results, shutdown, startup

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
    functions = [init_file, download_content, process_results]
    on_startup = startup
    on_shutdown = shutdown
    queue_name = NDC_QUEUE_NAME
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = msgpack.packb
    job_deserializer = lambda b: msgpack.unpackb(b, raw=False)


class Labeling:
    functions = [download_label_content, process_label_results, init_label_file]
    on_startup = label_startup
    on_shutdown = label_shutdown
    queue_name = LABEL_QUEUE_NAME
    queue_read_limit = 10
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = msgpack.packb
    job_deserializer = lambda b: msgpack.unpackb(b, raw=False)


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


process_group.add_command(ndc)
process_group.add_command(label)
