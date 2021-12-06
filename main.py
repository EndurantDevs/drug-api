#!/usr/bin/env python
import os
import logging.config
from pathlib import Path
import yaml
import click
import uvloop

from dotenv import load_dotenv
import arq.cli

from db.migrator import db_group
from process import process_group

uvloop.install()


@click.group()
def cli():
    pass


cli.add_command(process_group, name="start")
cli.add_command(db_group, name="db")
cli.add_command(arq.cli.cli, name="worker")


if __name__ == '__main__':
    env_path = Path(__file__).absolute().parent / '.env'
    load_dotenv(dotenv_path=env_path)
    with open(os.environ['LOG_CFG'], encoding="utf-8") as fobj:
        logging.config.dictConfig(yaml.safe_load(fobj))
    cli()
