import os
import asyncio
import uvloop
import click

from arq.connections import RedisSettings

from process.ndc_product import download_content, process_results, init_file, startup, shutdown
from process.ndc_product import main as initiate_product_import

from process.label import download_label_content, process_label_results, \
    init_label_file, label_startup, label_shutdown
from process.label import main as initiate_label_import

uvloop.install()


class NDC:
    functions = [init_file, download_content, process_results]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))


class Labeling:
    functions = [download_label_content, process_label_results, init_label_file]
    on_startup = label_startup
    on_shutdown = label_shutdown
    queue_read_limit = 10
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))


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
