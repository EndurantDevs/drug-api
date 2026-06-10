import datetime
import importlib

import pytest

from db.connection import Database

drug_indications = importlib.import_module("process.drug_indications")
label = importlib.import_module("process.label")
ndc_product = importlib.import_module("process.ndc_product")


class _Func:
    def count(self, *_args, **_kwargs):
        return "count"


class _ScalarQuery:
    def __init__(self, values):
        self._values = values

    async def scalar(self):
        return self._values.pop(0)


class _RecordingTransaction:
    def __init__(self, db):
        self._db = db

    async def __aenter__(self):
        self._db.events.append(("begin", None))
        return self._db

    async def __aexit__(self, exc_type, _exc, _tb):
        self._db.events.append(("rollback" if exc_type else "commit", None))
        return False


class _RecordingDb:
    def __init__(self, scalar_values=()):
        self.created_tables = []
        self.events = []
        self.func = _Func()
        self.scalar_values = list(scalar_values)
        self.statements = []

    async def status(self, statement):
        self.statements.append(statement)
        self.events.append(("status", statement))

    async def create_table(self, table, **_kwargs):
        self.created_tables.append(table)
        self.events.append(("create_table", table.name))

    def select(self, *_args, **_kwargs):
        return _ScalarQuery(self.scalar_values)

    def transaction(self):
        return _RecordingTransaction(self)


class _FakeSession:
    def __init__(self):
        self.closed = False

    def in_transaction(self):
        return False

    async def commit(self):
        pass

    async def rollback(self):
        pass

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_database_session_reuses_active_async_session():
    session = _FakeSession()
    db = Database()
    db.session_factory = lambda: session

    async with db.session() as outer:
        async with db.session() as inner:
            assert inner is outer

    assert session.closed is True


@pytest.mark.asyncio
async def test_ndc_startup_creates_suffixed_product_and_package_tables(monkeypatch):
    fake_db = _RecordingDb()

    async def fake_init_db(*_args, **_kwargs):
        pass

    monkeypatch.setattr(ndc_product, "db", fake_db)
    monkeypatch.setattr(ndc_product, "init_db", fake_init_db)

    ctx = {}
    await ndc_product.startup(ctx)

    import_date = ctx["import_date"]
    assert [table.name for table in fake_db.created_tables] == [
        f"product_{import_date}",
        f"package_{import_date}",
    ]
    assert f"DROP TABLE IF EXISTS rx_data.product_{import_date};" in fake_db.statements
    assert f"DROP TABLE IF EXISTS rx_data.package_{import_date};" in fake_db.statements


@pytest.mark.asyncio
async def test_ndc_shutdown_publishes_suffixed_tables_inside_transactions(monkeypatch):
    fake_db = _RecordingDb([100, 100, 200])

    async def fake_mark_control_run(*_args, **_kwargs):
        pass

    monkeypatch.setattr(ndc_product, "db", fake_db)
    monkeypatch.setattr(ndc_product, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(ndc_product, "print_time_info", lambda *_args, **_kwargs: None)

    ctx = {
        "import_date": "20260213",
        "context": {
            "product_count": 100,
            "start": datetime.datetime(2026, 2, 13),
        },
    }

    await ndc_product._shutdown_impl(ctx)

    assert fake_db.events.count(("begin", None)) == 2
    assert fake_db.events.count(("commit", None)) == 2
    assert "ALTER TABLE IF EXISTS rx_data.product RENAME TO product_old;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.product_20260213 RENAME TO product;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.package RENAME TO package_old;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.package_20260213 RENAME TO package;" in fake_db.statements


@pytest.mark.asyncio
async def test_label_startup_creates_suffixed_label_table(monkeypatch):
    fake_db = _RecordingDb()

    async def fake_init_db(*_args, **_kwargs):
        pass

    monkeypatch.setattr(label, "db", fake_db)
    monkeypatch.setattr(label, "init_db", fake_init_db)

    ctx = {}
    await label.label_startup(ctx)

    import_date = ctx["import_date"]
    assert [table.name for table in fake_db.created_tables] == [f"label_{import_date}"]
    assert f"DROP TABLE IF EXISTS rx_data.label_{import_date};" in fake_db.statements


@pytest.mark.asyncio
async def test_label_shutdown_publishes_suffixed_label_table_inside_transaction(monkeypatch):
    fake_db = _RecordingDb([25, 25])

    async def fake_mark_control_run(*_args, **_kwargs):
        pass

    monkeypatch.setattr(label, "db", fake_db)
    monkeypatch.setattr(label, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(label, "print_time_info", lambda *_args, **_kwargs: None)

    ctx = {
        "import_date": "20260213",
        "context": {
            "label_count": 25,
            "start": datetime.datetime(2026, 2, 13),
        },
    }

    await label._label_shutdown_impl(ctx)

    assert fake_db.events.count(("begin", None)) == 1
    assert fake_db.events.count(("commit", None)) == 1
    assert "ALTER TABLE IF EXISTS rx_data.label RENAME TO label_old;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.label_20260213 RENAME TO label;" in fake_db.statements


@pytest.mark.asyncio
async def test_drug_indications_publish_switches_staging_table(monkeypatch):
    fake_db = _RecordingDb()

    monkeypatch.setattr(drug_indications, "db", fake_db)

    await drug_indications._publish("rx_data", "20260213")

    assert "DROP TABLE IF EXISTS rx_data.drug_condition_evidence;" in fake_db.statements
    assert (
        "ALTER TABLE IF EXISTS rx_data.drug_condition_evidence_20260213 "
        "RENAME TO drug_condition_evidence;"
    ) in fake_db.statements
