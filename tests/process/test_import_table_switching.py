import datetime
import importlib
import os
import uuid

import pytest

from db.connection import Database

drug_indications = importlib.import_module("process.drug_indications")
label = importlib.import_module("process.label")
ndc_product = importlib.import_module("process.ndc_product")
ndc_publish = importlib.import_module("process.ndc_publish")

_NDC_INDEXES = (
    ("product", "product_idx_product_ndc", "product_ndc"),
    ("product", "product_idx_brand_trgm_idx", "brand_name gin_trgm_ops"),
    ("product", "product_idx_generic_trgm_idx", "generic_name gin_trgm_ops"),
    ("product", "product_rxnorm_idx", "rxnorm_ids"),
    ("package", "package_idx_product_ndc", "product_ndc"),
)


async def _seed_ndc_generations(database, schema):
    for suffix, generation in (("", "current"), ("_old", "previous"), ("_20260909", "incoming")):
        await database.status(
            f"CREATE TABLE {schema}.product{suffix} (product_ndc text, brand_name text, "
            "generic_name text, rxnorm_ids text[], generation text)"
        )
        await database.status(
            f"CREATE TABLE {schema}.package{suffix} (product_ndc text, generation text)"
        )
        await database.status(
            f"INSERT INTO {schema}.product{suffix} VALUES "
            "('00001-0001', 'Example', 'Example', ARRAY['1'], :generation)",
            generation=generation,
        )
        await database.status(
            f"INSERT INTO {schema}.package{suffix} VALUES ('00001-0001', :generation)",
            generation=generation,
        )
        if generation == "incoming":
            continue
        for table, index, columns in _NDC_INDEXES:
            await database.status(
                f"CREATE INDEX {index}{suffix} ON {schema}.{table}{suffix} USING GIN({columns})"
            )


@pytest.fixture
async def ndc_publication_database():
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if os.getenv("HLTHPRT_ENVIRONMENT") != "test" or not (
        "test" in database_name or database_name.endswith("_ci")
    ):
        pytest.skip("requires an explicitly configured disposable PostgreSQL test database")
    database = Database()
    schema = f"ndc_publish_test_{uuid.uuid4().hex}"
    is_schema_created = False
    try:
        await database.connect()
        for extension in ("pg_trgm", "btree_gin"):
            await database.status(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        await database.status(f"CREATE SCHEMA {schema}")
        is_schema_created = True
        await _seed_ndc_generations(database, schema)
        yield database, schema
    finally:
        try:
            if is_schema_created:
                await database.status(f"DROP SCHEMA {schema} CASCADE")
        finally:
            await database.disconnect()


async def _ndc_generation_state(database, schema, suffix):
    state_by_table = {}
    for table in ("product", "package"):
        relation = f"{schema}.{table}{suffix}"
        state_by_table[table] = {
            "oid": await database.scalar("SELECT CAST(:name AS regclass)::oid", name=relation),
            "generation": await database.scalar(f"SELECT generation FROM {relation}"),
            "indexes": tuple((await database.execute(
                "SELECT c.relname, c.oid, i.indisvalid FROM pg_index i "
                "JOIN pg_class c ON c.oid = i.indexrelid "
                "WHERE i.indrelid = CAST(:name AS regclass) ORDER BY c.relname",
                name=relation,
            )).all()),
        }
    return state_by_table


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_package", [False, True], ids=["success", "package_failure"])
async def test_ndc_publication_preserves_generations_and_indexes(ndc_publication_database, monkeypatch, fail_package):
    database, schema = ndc_publication_database
    before_by_suffix = {suffix: await _ndc_generation_state(database, schema, suffix)
                        for suffix in ("", "_old", "_20260909")}
    publish_single = ndc_publish._publish_single_ndc_table

    async def fail_after_product(database, db_schema, table, import_date):
        if table == "package":
            assert await database.scalar(f"SELECT generation FROM {schema}.product") == "incoming"
            raise RuntimeError("injected package publication failure")
        await publish_single(database, db_schema, table, import_date)

    if fail_package:
        monkeypatch.setattr(ndc_publish, "_publish_single_ndc_table", fail_after_product)
        with pytest.raises(RuntimeError, match="injected package publication failure"):
            await ndc_publish.publish_ndc_tables(database, schema, "20260909")
        assert await _ndc_generation_state(database, schema, "") == before_by_suffix[""]
        assert await _ndc_generation_state(database, schema, "_old") == before_by_suffix["_old"]
        assert await _ndc_generation_state(database, schema, "_20260909") == before_by_suffix["_20260909"]
        monkeypatch.setattr(ndc_publish, "_publish_single_ndc_table", publish_single)

    await ndc_publish.publish_ndc_tables(database, schema, "20260909")
    incoming = await _ndc_generation_state(database, schema, "")
    previous = await _ndc_generation_state(database, schema, "_old")
    for table in ("product", "package"):
        assert previous[table]["oid"] == before_by_suffix[""][table]["oid"]
        assert previous[table]["generation"] == "current"
        assert tuple((name.removesuffix("_old"), oid, valid)
                     for name, oid, valid in previous[table]["indexes"]) == before_by_suffix[""][table]["indexes"]
        assert await database.scalar("SELECT to_regclass(:name)", name=f"{schema}.{table}_20260909") is None
        removed_oids = [before_by_suffix["_old"][table]["oid"]] + [
            index_row[1] for index_row in before_by_suffix["_old"][table]["indexes"]
        ]
        for oid in removed_oids:
            assert not await database.scalar("SELECT 1 FROM pg_class WHERE oid = :oid", oid=oid)
        assert incoming[table]["oid"] == before_by_suffix["_20260909"][table]["oid"]
        assert incoming[table]["generation"] == "incoming"
        assert [index_row[0] for index_row in incoming[table]["indexes"]] == sorted(
            index for owner, index, _ in _NDC_INDEXES if owner == table
        )
        assert all(index_row[2] for index_row in incoming[table]["indexes"])


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
        return None

    async def rollback(self):
        return None

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
async def test_ndc_startup_does_not_touch_stage_tables(monkeypatch):
    fake_db = _RecordingDb()

    async def fake_init_db(*_args, **_kwargs):
        return None

    monkeypatch.setattr(ndc_product, "db", fake_db)
    monkeypatch.setattr(ndc_product, "init_db", fake_init_db)
    await ndc_product.startup({})
    assert not fake_db.created_tables
    assert not fake_db.statements


@pytest.mark.asyncio
async def test_ndc_legacy_publication_indexes_stages_before_serving_changes_in_one_transaction():
    fake_db = _RecordingDb()
    await ndc_publish.publish_ndc_tables(fake_db, "rx_data", "20260213")
    assert fake_db.events.count(("begin", None)) == 1
    assert fake_db.events.count(("commit", None)) == 1
    transaction_start = fake_db.events.index(("begin", None))
    index_positions = [index for index, (event, statement) in enumerate(fake_db.events)
                       if event == "status" and statement.startswith("CREATE INDEX ")]
    first_serving_change = fake_db.events.index(("status", "DROP TABLE IF EXISTS rx_data.product_old;"))
    assert len(index_positions) == 5
    assert transaction_start < min(index_positions) <= max(index_positions) < first_serving_change
    assert "ALTER TABLE IF EXISTS rx_data.product RENAME TO product_old;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.product_20260213 RENAME TO product;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.package RENAME TO package_old;" in fake_db.statements
    assert "ALTER TABLE IF EXISTS rx_data.package_20260213 RENAME TO package;" in fake_db.statements


@pytest.mark.asyncio
async def test_label_startup_creates_suffixed_label_table(monkeypatch):
    fake_db = _RecordingDb()

    async def fake_init_db(*_args, **_kwargs):
        return None

    monkeypatch.setattr(label, "db", fake_db)
    monkeypatch.setattr(label, "init_db", fake_init_db)

    context_dict = {}
    await label.label_startup(context_dict)

    import_date = context_dict["import_date"]
    assert [table.name for table in fake_db.created_tables] == [f"label_{import_date}"]
    assert f"DROP TABLE IF EXISTS rx_data.label_{import_date};" in fake_db.statements


@pytest.mark.asyncio
async def test_label_shutdown_publishes_suffixed_label_table_inside_transaction(monkeypatch):
    fake_db = _RecordingDb([25, 25])

    async def fake_mark_control_run(*_args, **_kwargs):
        return None

    monkeypatch.setattr(label, "db", fake_db)
    monkeypatch.setattr(label, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(label, "print_time_info", lambda *_args, **_kwargs: None)

    context_dict = {
        "import_date": "20260213",
        "context": {
            "label_count": 25,
            "start": datetime.datetime(2026, 2, 13),
        },
    }

    await label._label_shutdown_impl(context_dict)

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
