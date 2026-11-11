"""Report failed Label burst jobs through the existing worker command."""

import asyncio
import logging.config
import os
import sys

import arq.cli
import click
from arq.logs import default_log_config
from arq.utils import import_string
from arq.worker import FailedJobs, create_worker


async def _run_label_burst(settings):
    worker = create_worker(settings, burst=True)
    try:
        await worker.async_run()
        if worker.jobs_failed:
            raise FailedJobs(worker.jobs_failed, [])
    finally:
        await worker.close()


def _worker_cli(*, worker_settings, burst, check, watch, verbose, custom_log_dict):
    if worker_settings != "process.Labeling" or burst is not True or check or watch:
        return arq.cli.cli.callback(
            worker_settings=worker_settings,
            burst=burst,
            check=check,
            watch=watch,
            verbose=verbose,
            custom_log_dict=custom_log_dict,
        )
    sys.path.append(os.getcwd())
    settings = import_string(worker_settings)
    log_config = import_string(custom_log_dict) if custom_log_dict else default_log_config(verbose)
    logging.config.dictConfig(log_config)
    asyncio.run(_run_label_burst(settings))


worker_cli = click.Command(
    arq.cli.cli.name,
    callback=_worker_cli,
    params=arq.cli.cli.params,
    help=arq.cli.cli.help,
)
