# pylint: disable = unused-variable
import os
from gino import GinoStrategy
from gino.api import Gino
from sqlalchemy.engine.url import URL

GinoStrategy()


async def init_db(db, loop):
    dsn = URL(
        drivername=os.environ.get('DB_DRIVER', 'asyncpg'),
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', 5432),
        username=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', ''),
        database=os.environ.get('DB_DATABASE', 'postgres'),
    )
    await db.set_bind(
        dsn,
        echo=os.environ.get('DB_ECHO', False),
        min_size=int(os.environ.get('DB_POOL_MIN_SIZE', 5)),
        max_size=int(os.environ.get('DB_POOL_MAX_SIZE', 10)),
        ssl=bool(os.environ.get('DB_SSL', False)),
        loop=loop,
        **os.environ.get('DB_KWARGS', {}),
    )


db = Gino()
