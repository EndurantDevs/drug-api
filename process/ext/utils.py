import datetime
import hashlib

import httpx
import humanize
from aiofile import async_open
from arq import Retry
from asyncpg.exceptions import UniqueViolationError
from sqlalchemy.exc import SQLAlchemyError

from db.connection import Base

HTTP_CHUNK_SIZE = 512 * 1024
headers = {'user-agent': 'Healthporta Drug API Importer, https://github.com/EndurantDevs/drug-api'}
_MODEL_CACHE = {}


async def download_it(url):
    """Download a URL with a small retrying HTTPX client."""
    transport = httpx.AsyncHTTPTransport(retries=3)
    timeout = httpx.Timeout(5)
    async with httpx.AsyncClient(transport=transport, timeout=timeout, headers=headers) as client:
        r = await client.get(url)
        return r


async def download_it_and_save(url, filepath, *, max_bytes=None):
    """Stream a URL into a local file, asking ARQ to retry transient failures."""
    transport = httpx.AsyncHTTPTransport(retries=3)
    timeout = httpx.Timeout(10)
    async with async_open(filepath, 'wb+') as afp:
        async with httpx.AsyncClient(timeout=timeout, transport=transport, headers=headers) as client:
            async with client.stream('GET', url) as response:
                receipt = await _write_response_chunks(response, afp, max_bytes=max_bytes)
                return {"url": str(response.url), **receipt}


async def _write_response_chunks(response, afp, *, max_bytes=None) -> dict:
    if response.status_code != 200:
        raise Retry()
    digest = hashlib.sha256()
    byte_count = 0
    try:
        async for chunk in response.aiter_bytes(chunk_size=HTTP_CHUNK_SIZE):
            byte_count += len(chunk)
            if max_bytes is not None and byte_count > max_bytes:
                raise ValueError("download exceeds byte limit")
            await afp.write(chunk)
            digest.update(chunk)
    except (httpx.TimeoutException, httpx.ReadError, httpx.NetworkError):
        raise Retry()
    return {"sha256": digest.hexdigest(), "size_bytes": byte_count}


def make_class(model_cls, table_suffix):
    """Return a cached model class for a suffixed copy of a table."""
    key = (model_cls, str(table_suffix))
    if key in _MODEL_CACHE:
        return _MODEL_CACHE[key]

    table_name = '_'.join([model_cls.__tablename__, str(table_suffix)])
    source_table = model_cls.__table__
    table_key = f"{source_table.schema}.{table_name}" if source_table.schema else table_name
    table = Base.metadata.tables.get(table_key)
    if table is None:
        table = source_table.to_metadata(Base.metadata, name=table_name, schema=source_table.schema)

    cls = type(
        f"{model_cls.__name__}_{table_suffix}",
        (Base,),
        {
            "__module__": model_cls.__module__,
            "__table__": table,
        },
    )
    cls.__tablename__ = table_name
    _MODEL_CACHE[key] = cls
    return cls


async def push_objects(obj_list, cls):
    """Bulk insert objects, falling back to row-by-row inserts on conflicts."""
    if obj_list:
        try:
            await cls.insert().all(obj_list)
        except (SQLAlchemyError, UniqueViolationError):
            for obj in obj_list:
                try:
                    await cls.insert().all([obj])
                except (SQLAlchemyError, UniqueViolationError) as exc:
                    print(exc)


def print_time_info(start):
    """Print a human-readable elapsed import duration."""
    now = datetime.datetime.now()
    delta = now - start
    print('Import Time Delta: ', delta)
    print('Import took ', humanize.naturaldelta(delta))
