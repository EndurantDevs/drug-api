"""Database readiness must precede HTTP serving and queue consumption."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from arq import worker as arq_worker
from sanic import Sanic, response
from sanic.application.constants import ServerStage
from sanic.application.state import ApplicationServerInfo
from sanic.server import runners

from db import connection as database_connection
from process import NDC, DrugIndications, Labeling


@pytest.fixture
def candidate_engine(monkeypatch):
    engine = MagicMock()
    engine.dispose = AsyncMock()
    engine.connect.return_value.__aenter__.return_value.execute = AsyncMock()
    monkeypatch.setattr(database_connection, "create_async_engine", Mock(return_value=engine))
    return engine


@pytest.mark.asyncio
async def test_connect_publishes_only_after_probe(monkeypatch, candidate_engine):
    database = database_connection.Database()
    entered = asyncio.Event()
    release = asyncio.Event()
    session_factory = object()
    factory = Mock(return_value=session_factory)
    monkeypatch.setattr(database_connection, "async_sessionmaker", factory)

    async def probe(statement):
        assert str(statement) == "SELECT 1"
        assert database.engine is None and database.session_factory is None
        factory.assert_not_called()
        entered.set()
        await release.wait()

    candidate_engine.connect.return_value.__aenter__.return_value.execute.side_effect = probe
    async with asyncio.TaskGroup() as tasks:
        first = tasks.create_task(database.connect())
        await asyncio.wait_for(entered.wait(), timeout=1)
        second = tasks.create_task(database.connect())
        await asyncio.sleep(0)
        assert not first.done() and not second.done()
        database_connection.create_async_engine.assert_called_once()
        release.set()

    assert database.engine is candidate_engine and database.session_factory is session_factory
    factory.assert_called_once_with(candidate_engine, expire_on_commit=False, autoflush=False)
    candidate_engine.connect.return_value.__aexit__.assert_awaited_once()
    await database.connect()
    candidate_engine.connect.assert_called_once()
    candidate_engine.dispose.assert_not_awaited()
    await database.disconnect()
    await database.disconnect()
    candidate_engine.dispose.assert_awaited_once()
    assert database.engine is None and database.session_factory is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["connect", "probe", "release", "factory"])
async def test_failed_connect_disposes_and_retries(monkeypatch, candidate_engine, failure_stage):
    database = database_connection.Database()
    lease = candidate_engine.connect.return_value
    factory = Mock(return_value=object())
    monkeypatch.setattr(database_connection, "async_sessionmaker", factory)
    failure_point = {
        "connect": lease.__aenter__,
        "probe": lease.__aenter__.return_value.execute,
        "release": lease.__aexit__,
        "factory": factory,
    }[failure_stage]
    failure_point.side_effect = ConnectionError("database unavailable")

    with pytest.raises(ConnectionError, match="database unavailable"):
        await database.connect()

    assert database.engine is None and database.session_factory is None
    candidate_engine.dispose.assert_awaited_once()
    failure_point.side_effect = None
    replacement = MagicMock()
    replacement.connect.return_value.__aenter__.return_value.execute = AsyncMock()
    database_connection.create_async_engine.return_value = replacement
    await database.connect()
    assert database.engine is replacement and database.session_factory is factory.return_value


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_stage", ["connect", "probe"])
async def test_cancelled_connect_disposes_and_retries(candidate_engine, cancel_stage):
    database = database_connection.Database()
    entered = asyncio.Event()
    lease = candidate_engine.connect.return_value
    pending_call = lease.__aenter__ if cancel_stage == "connect" else lease.__aenter__.return_value.execute

    async def pending(*_args):
        entered.set()
        await asyncio.Event().wait()

    pending_call.side_effect = pending
    task = asyncio.create_task(database.connect())
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    candidate_engine.dispose.assert_awaited_once()
    assert database.engine is None and database.session_factory is None
    pending_call.side_effect = None
    await database.connect()
    assert database.engine is candidate_engine and database.session_factory is not None


@pytest.mark.asyncio
async def test_disconnect_waits_for_pending_connect(candidate_engine):
    database = database_connection.Database()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def probe(_statement):
        entered.set()
        await release.wait()

    candidate_engine.connect.return_value.__aenter__.return_value.execute.side_effect = probe
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(database.connect())
        await asyncio.wait_for(entered.wait(), timeout=1)
        disconnect = tasks.create_task(database.disconnect())
        await asyncio.sleep(0)
        assert not disconnect.done()
        release.set()
    assert database.engine is None and database.session_factory is None
    candidate_engine.dispose.assert_awaited_once()


@pytest.mark.parametrize("fails", [False, True])
def test_sanic_probes_before_serving(monkeypatch, candidate_engine, fails):
    database = database_connection.Database()
    app = Sanic(f"database-startup-{fails}", configure_logging=False)
    app.config.TOUCHUP = False
    app.state.server_info.append(ApplicationServerInfo(settings={}, stage=ServerStage.SERVING))
    app.get("/")(lambda _request: response.text("ready"))
    database.init_app(app)
    loop = asyncio.new_event_loop()
    events = []

    async def probe(_statement):
        events.append("probe")
        if fails:
            raise ConnectionError("database unavailable")

    async def accept_connections():
        assert database.engine is candidate_engine and database.session_factory is not None
        events.append("serve")
        return MagicMock()

    serve = AsyncMock(side_effect=accept_connections)
    server_coroutine = serve()
    candidate_engine.connect.return_value.__aenter__.return_value.execute.side_effect = probe
    monkeypatch.setattr(loop, "create_server", Mock(return_value=server_coroutine))
    monkeypatch.setattr(runners, "_setup_system_signals", Mock())
    monkeypatch.setattr(runners, "_run_server_forever", Mock())
    try:
        if fails:
            with pytest.raises(ConnectionError, match="database unavailable"):
                runners.serve("127.0.0.1", 0, app, loop=loop, register_sys_signals=False)
            serve.assert_not_awaited()
            candidate_engine.dispose.assert_awaited_once()
            assert database.engine is None and database.session_factory is None
        else:
            runners.serve("127.0.0.1", 0, app, loop=loop, register_sys_signals=False)
            serve.assert_awaited_once()
            assert events == ["probe", "serve"]
    finally:
        server_coroutine.close()
        loop.run_until_complete(database.disconnect())
        loop.close()
        Sanic.unregister_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize("settings", [NDC, Labeling, DrugIndications])
async def test_worker_startup_refuses_before_polling(monkeypatch, candidate_engine, settings):
    database = database_connection.Database()
    monkeypatch.setattr(database_connection, "db", database)
    candidate_engine.connect.return_value.__aenter__.side_effect = ConnectionError("database unavailable")
    monkeypatch.setattr(arq_worker, "log_redis_info", AsyncMock())
    worker = arq_worker.create_worker(settings, redis_pool=MagicMock(), handle_signals=False)
    monkeypatch.setattr(worker, "_poll_iteration", AsyncMock(side_effect=AssertionError("unexpected queue polling")))

    with pytest.raises(ConnectionError, match="database unavailable"):
        await worker.main()

    worker._poll_iteration.assert_not_awaited()
    candidate_engine.dispose.assert_awaited_once()
    assert database.engine is None and database.session_factory is None
