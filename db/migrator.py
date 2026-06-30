from pathlib import Path

import click
from dotenv import load_dotenv

from alembic.command import current as show_current
from alembic.command import downgrade as make_downgrade
from alembic.command import history as show_history
from alembic.command import revision, upgrade
from alembic.config import Config

BASE_DIR = (Path(__file__).parent / '..').absolute()
ALEMBIC_INI = BASE_DIR / 'alembic.ini'


@click.group()
def db_group():
    """Group database migration commands."""


@click.command(help="Downgrade to revision")
@click.option("-r", help="Revision", default="-1")
def downgrade(r):
    """Downgrade the database to the requested Alembic revision."""
    alembic_cfg = Config(ALEMBIC_INI)
    make_downgrade(alembic_cfg, r)


@click.command(help="Apply migrations")
@click.option("-r", help="Revision (head by default)", default="head")
def migrate(r):
    """Apply Alembic migrations through the requested revision."""
    alembic_cfg = Config(ALEMBIC_INI)
    upgrade(alembic_cfg, r)


@click.command(help="Auto generate migrations")
@click.option("-m", help="Migration message", required=True)
def generate(m):
    """Generate an Alembic migration from model metadata changes."""
    alembic_cfg = Config(ALEMBIC_INI)
    revision_kwargs_dict = {'autogenerate': True}
    if m is not None:
        revision_kwargs_dict['message'] = m
    revision(alembic_cfg, **revision_kwargs_dict)


@click.command(help="List changeset scripts in chronological order")
def history():
    """Show Alembic migration history."""
    alembic_cfg = Config(ALEMBIC_INI)
    show_history(alembic_cfg)


@click.command(help="Show current revision")
def current():
    """Show the current Alembic revision."""
    alembic_cfg = Config(ALEMBIC_INI)
    show_current(alembic_cfg)


db_group.add_command(migrate)
db_group.add_command(generate)
db_group.add_command(history)
db_group.add_command(current)
db_group.add_command(downgrade)


if __name__ == "__main__":
    env_path = BASE_DIR / '.env'
    load_dotenv(dotenv_path=env_path)
    db_group()
