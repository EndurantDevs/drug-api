import datetime
import httpx
from gino.exceptions import GinoException
from asyncpg.exceptions import UniqueViolationError
import ijson
from aiofile import async_open
from arq import Retry
import humanize

HTTP_CHUNK_SIZE = 512 * 1024
headers = {'user-agent': 'Healthporta Drug API Importer, https://github.com/EndurantDevs/drug-api'}


async def download_it(url):
    transport = httpx.AsyncHTTPTransport(retries=3)
    timeout = httpx.Timeout(5)
    async with httpx.AsyncClient(transport=transport, timeout=timeout, headers=headers) as client:
        r = await client.get(url)
        return r


async def download_it_and_save(url, filepath):
    transport = httpx.AsyncHTTPTransport(retries=3)
    timeout = httpx.Timeout(10)
    async with async_open(filepath, 'wb+') as afp:
        async with httpx.AsyncClient(timeout=timeout, transport=transport, headers=headers) as client:
            async with client.stream('GET', url) as response:
                if response.status_code == 200:
                    try:
                        async for chunk in response.aiter_bytes(chunk_size=HTTP_CHUNK_SIZE):
                            await afp.write(chunk)
                    except (httpx.TimeoutException, httpx.ReadError, httpx.NetworkError):
                        raise Retry()
                else:
                    raise Retry()


def make_class(Base, table_suffix):
    temp = None
    if hasattr(Base, '__table__'):
        try:
            temp = Base.__table__
            delattr(Base, '__table__')
        except AttributeError:
            pass

    class MyClass(Base):
        __tablename__ = '_'.join([Base.__tablename__, table_suffix])

    if temp is not None:
        Base.__table__ = temp

    return MyClass


async def push_objects(obj_list, cls):
    if obj_list:
        try:
            await cls.insert().gino.all(obj_list)
        except (GinoException, UniqueViolationError):
            for obj in obj_list:
                try:
                    await cls.insert().gino.all([obj])
                except (GinoException, UniqueViolationError) as e:
                    print(e)


def print_time_info(start):
    now = datetime.datetime.now()
    delta = now - start
    print('Import Time Delta: ', delta)
    print('Import took ', humanize.naturaldelta(now, when=start))
