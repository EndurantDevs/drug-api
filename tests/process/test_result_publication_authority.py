import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

authority = importlib.import_module("process.result_publication_authority")
label_publish = importlib.import_module("process.label_publish")
drug_indications = importlib.import_module("process.drug_indications")
_LINEAGE = "c8f27af1-56ba-4cda-82d8-0fc67650918f"


def _generation(value=4):
    return {
        "origin_lineage_id": _LINEAGE,
        "origin_generation": value,
        "published_at": "2026-09-21T08:30:00Z",
    }


def _dependencies():
    return authority.indication_dependencies(
        authority.dependency_entry("label", (("label", 11),), generation=_generation()),
        authority.dependency_entry("ndc", (("product", 12),)),
        authority.dependency_entry(
            "clinical-reference",
            (("code_relationship", 21), ("code_catalog", 22), ("code_synonym", 23)),
        ),
    )


def test_indication_dependencies_bind_every_exact_consumed_relation():
    dependencies = _dependencies()

    assert dependencies["format"] == authority.DEPENDENCY_FORMAT
    assert tuple(dependencies) == ("format", "label", "ndc", "clinical-reference")
    assert dependencies["label"]["generation"]["origin_generation"] == 4
    assert [relation["oid"] for relation in dependencies["clinical-reference"]["relations"]] == [21, 22, 23]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(extra={}),
        lambda value: value["ndc"]["relations"].append({"name": "package", "oid": 13}),
        lambda value: value["clinical-reference"].update(generation=_generation()),
    ],
)
def test_indication_dependencies_reject_unbounded_or_misbound_inputs(mutate):
    value = _dependencies()
    mutate(value)
    with pytest.raises(ValueError, match="consumed dependenc"):
        authority.validate_consumed_dependencies("drug-indications", value)


@pytest.mark.parametrize(
    "authority_row",
    [
        {
            "importer_id": "label",
            "local_lineage_id": _LINEAGE,
            "local_generation": 1,
            "origin_lineage_id": None,
            "origin_generation": None,
            "published_at": None,
            "relation_oids": None,
            "consumed_dependencies": None,
        },
        {
            "importer_id": "label",
            "local_lineage_id": _LINEAGE,
            "local_generation": 0,
            "origin_lineage_id": _LINEAGE,
            "origin_generation": 1,
            "published_at": "2026-09-21T08:30:00Z",
            "relation_oids": [11],
            "consumed_dependencies": {},
        },
    ],
)
def test_authority_rejects_generation_state_that_disagrees_with_serving_state(authority_row):
    with pytest.raises(RuntimeError, match="generation-zero|serving generation"):
        authority.validate_result_publication_authority(authority_row)


class _Transaction:
    def __init__(self, database):
        self.database = database

    async def __aenter__(self):
        self.database.events.append("begin")

    async def __aexit__(self, exc_type, _exc, _tb):
        self.database.events.append("rollback" if exc_type else "commit")


class _Database:
    def __init__(self):
        self.events = []
        self.statements = []

    def transaction(self):
        return _Transaction(self)

    async def status(self, statement):
        self.statements.append(statement)
        self.events.append("ddl")

    async def first(self, *_args, **_kwargs):
        return {"allowed": True}


@pytest.mark.asyncio
async def test_label_publication_advances_authority_before_atomic_commit(monkeypatch):
    database = _Database()

    async def advance(*_args, **kwargs):
        assert kwargs == {"importer_id": "label", "schema": "rx_data", "consumed_dependencies": {}}
        database.events.append("authority")

    monkeypatch.setattr(label_publish, "publish_local_result_generation", advance)
    await label_publish.publish_label_table(database, "rx_data", "20260921")

    assert database.events[0] == "begin"
    assert database.events[-2:] == ["authority", "commit"]
    assert "ALTER TABLE IF EXISTS rx_data.label_20260921 RENAME TO label;" in database.statements
    timeout_index = database.statements.index("SET LOCAL lock_timeout = '5s'")
    first_live_ddl_index = database.statements.index("DROP TABLE IF EXISTS rx_data.label_old;")
    assert timeout_index < first_live_ddl_index


@pytest.mark.asyncio
async def test_label_publication_authority_failure_rolls_back_the_swap(monkeypatch):
    database = _Database()

    async def fail(*_args, **_kwargs):
        raise RuntimeError("injected authority failure")

    monkeypatch.setattr(label_publish, "publish_local_result_generation", fail)
    with pytest.raises(RuntimeError, match="injected authority failure"):
        await label_publish.publish_label_table(database, "rx_data", "20260921")

    assert database.events[-1] == "rollback"
    assert "commit" not in database.events


@pytest.mark.asyncio
async def test_ordinary_label_finalizer_rejects_protected_heap_before_ddl(monkeypatch):
    database = _Database()
    monkeypatch.setattr(database, "first", AsyncMock(return_value={"allowed": False}))
    with pytest.raises(RuntimeError, match="ordinary Label publication is disabled"):
        await label_publish.publish_label_table(database, "rx_data", "20260921")
    assert database.statements == []
    assert database.events == ["begin", "rollback"]


@pytest.mark.asyncio
async def test_indication_publication_binds_dependencies_in_its_swap_transaction(monkeypatch):
    database = _Database()
    dependencies = _dependencies()

    async def advance(*_args, **kwargs):
        assert kwargs == {
            "importer_id": "drug-indications",
            "schema": "rx_data",
            "consumed_dependencies": dependencies,
        }
        database.events.append("authority")

    monkeypatch.setattr(drug_indications, "db", database)
    monkeypatch.setattr(drug_indications, "publish_local_result_generation", advance)
    async with database.transaction():
        await drug_indications._publish("rx_data", "20260921", dependencies)

    assert database.events[-2:] == ["authority", "commit"]
    assert "DROP TABLE IF EXISTS rx_data.drug_condition_evidence;" in database.statements


@pytest.mark.asyncio
async def test_indication_authority_failure_rolls_back_the_swap(monkeypatch):
    database = _Database()

    async def fail(*_args, **_kwargs):
        raise RuntimeError("injected authority failure")

    monkeypatch.setattr(drug_indications, "db", database)
    monkeypatch.setattr(drug_indications, "publish_local_result_generation", fail)
    with pytest.raises(RuntimeError, match="injected authority failure"):
        async with database.transaction():
            await drug_indications._publish("rx_data", "20260921", _dependencies())

    assert database.events[-1] == "rollback"
    assert "commit" not in database.events


class _IdentityDatabase:
    def __init__(self, authority_oid=11):
        self.all_rows = [
            [{"relation_name": "label", "relation_oid": 11}],
            [{"relation_name": "product", "relation_oid": 12}],
        ]
        self.authority_oid = authority_oid
        self.lock = None

    async def status(self, statement):
        self.lock = statement

    async def all(self, *_args, **_kwargs):
        return self.all_rows.pop(0)

    async def first(self, *_args, **_kwargs):
        return {
            "importer_id": "label",
            "local_lineage_id": _LINEAGE,
            "local_generation": 4,
            "origin_lineage_id": _LINEAGE,
            "origin_generation": 4,
            "published_at": datetime.datetime(2026, 9, 21, 8, 30, tzinfo=datetime.UTC),
            "relation_oids": [self.authority_oid],
            "consumed_dependencies": {},
        }


@pytest.mark.asyncio
async def test_local_dependency_capture_locks_and_binds_matching_label_generation():
    database = _IdentityDatabase()
    label, ndc = await authority.local_indication_dependencies(database, "rx_data")

    assert "ACCESS SHARE MODE" in database.lock
    assert label == authority.dependency_entry("label", (("label", 11),), generation=_generation())
    assert ndc == authority.dependency_entry("ndc", (("product", 12),))


@pytest.mark.asyncio
async def test_local_dependency_capture_omits_a_drifted_portable_generation():
    label, ndc = await authority.local_indication_dependencies(_IdentityDatabase(authority_oid=99), "rx_data")

    assert label == authority.dependency_entry("label", (("label", 11),))
    assert ndc == authority.dependency_entry("ndc", (("product", 12),))


class _AuthorityDatabase:
    def __init__(self):
        self.first_rows = [
            {
                "importer_id": "label",
                "local_lineage_id": _LINEAGE,
                "local_generation": 0,
                "origin_lineage_id": None,
                "origin_generation": None,
                "published_at": None,
                "relation_oids": None,
                "consumed_dependencies": None,
            },
            {
                "importer_id": "label",
                "local_lineage_id": _LINEAGE,
                "local_generation": 1,
                "origin_lineage_id": _LINEAGE,
                "origin_generation": 1,
                "published_at": datetime.datetime(2026, 9, 21, 8, 30, tzinfo=datetime.UTC),
                "relation_oids": [31],
                "consumed_dependencies": {},
            },
        ]
        self.update_parameters = None

    async def first(self, statement, **parameters):
        if str(statement).startswith("UPDATE"):
            self.update_parameters = parameters
        return self.first_rows.pop(0)

    async def all(self, *_args, **_kwargs):
        return [{"relation_name": "label", "relation_oid": 31}]


@pytest.mark.asyncio
async def test_local_publication_advances_generation_and_binds_promoted_oid():
    database = _AuthorityDatabase()
    published = await authority.publish_local_result_generation(
        database,
        importer_id="label",
        schema="rx_data",
        consumed_dependencies={},
    )

    assert published.local_generation == 1
    assert published.serving_generation.origin_lineage_id == published.local_lineage_id
    assert published.relation_oids == (31,)
    assert database.update_parameters["next_generation"] == 1
    assert database.update_parameters["relation_oids"] == [31]


class _ClinicalTransaction:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        self.connection.transaction_events.append("begin")

    async def __aexit__(self, exc_type, _exc, _tb):
        self.connection.transaction_events.append("rollback" if exc_type else "commit")


class _ClinicalConnection:
    def __init__(self):
        self.fetch_calls = []
        self.lock_statement = None
        self.transaction_events = []

    def transaction(self):
        return _ClinicalTransaction(self)

    async def execute(self, statement):
        self.lock_statement = statement

    async def fetch(self, statement, *parameters):
        self.fetch_calls.append((statement, parameters))
        if len(self.fetch_calls) == 1:
            return [
                {"relation_name": "code_relationship", "oid": 21},
                {"relation_name": "code_catalog", "oid": 22},
                {"relation_name": "code_synonym", "oid": 23},
            ]
        if len(self.fetch_calls) == 2:
            return [{"rxcui": "1"}]
        return [{"term": "example"}]


@pytest.mark.asyncio
async def test_clinical_reads_bind_oids_and_rows_under_dml_conflicting_lock():
    connection = _ClinicalConnection()
    relationship_rows, term_rows, dependency = await drug_indications._read_clinical_rows(connection, "mrf")

    identity_sql, identity_parameters = connection.fetch_calls[0]
    assert "format('%I.%I', CAST($1 AS text), relation_name)" in identity_sql
    assert "AS oid" in identity_sql
    assert identity_parameters == ("mrf", list(authority.DEPENDENCY_RELATIONS["clinical-reference"]))
    assert connection.lock_statement.endswith(" IN SHARE MODE")
    assert connection.transaction_events == ["begin", "commit"]
    assert relationship_rows == [{"rxcui": "1"}]
    assert term_rows == [{"term": "example"}]
    assert dependency == authority.dependency_entry(
        "clinical-reference",
        (("code_relationship", 21), ("code_catalog", 22), ("code_synonym", 23)),
    )


class _BuildTransaction:
    def __init__(self, database):
        self.database = database

    async def __aenter__(self):
        assert not self.database.active
        self.database.active = True
        self.database.events.append("begin")

    async def __aexit__(self, exc_type, _exc, _tb):
        self.database.events.append("rollback" if exc_type else "commit")
        self.database.active = False


class _BuildDatabase:
    def __init__(self):
        self.active = False
        self.events = []
        self.func = type("Functions", (), {"count": staticmethod(lambda _column: 1)})()

    def transaction(self):
        return _BuildTransaction(self)

    def acquire(self):
        raise AssertionError("a separate dependency connection must not be acquired")

    def select(self, _expression):
        assert self.active

        class _Count:
            async def scalar(self):
                return 1

        return _Count()


@pytest.mark.asyncio
async def test_indication_scan_holds_one_context_transaction_without_a_dependency_connection(monkeypatch):
    database = _BuildDatabase()
    dependencies = _dependencies()

    async def local_inputs(selected_database, schema):
        assert selected_database is database and database.active and schema == "rx_data"
        return dependencies["label"], dependencies["ndc"]

    async def product_inputs():
        assert database.active
        return {}

    async def clinical_inputs(test_mode=False):
        assert database.active and test_mode
        return {}, dependencies["clinical-reference"]

    async def scan(*_args):
        assert database.active
        return 0, 0

    async def indexes(*_args):
        assert database.active

    evidence_cls = type("Evidence", (), {"evidence_id": object()})
    monkeypatch.setattr(drug_indications, "db", database)
    monkeypatch.setattr(drug_indications, "local_indication_dependencies", local_inputs)
    monkeypatch.setattr(drug_indications, "_rxnorm_ids_by_product", product_inputs)
    monkeypatch.setattr(drug_indications, "_load_official_condition_context", clinical_inputs)
    monkeypatch.setattr(drug_indications, "_scan_condition_evidence", scan)
    monkeypatch.setattr(drug_indications, "_create_indexes", indexes)
    monkeypatch.setenv("HLTHPRT_DRUG_INDICATIONS_MIN_ROWS", "0")
    monkeypatch.delenv("HLTHPRT_DRUG_INDICATIONS_PUBLISH_TEST_MODE", raising=False)

    build_outcome = await drug_indications._build_evidence_stage(
        evidence_cls, "rx_data", "20260921", 100, 10, True, None
    )

    assert build_outcome[:3] == (0, 0, 1)
    assert database.events == ["begin", "commit"]


@pytest.mark.asyncio
async def test_indication_publication_rejects_missing_clinical_identity_before_scan(monkeypatch):
    database = _BuildDatabase()

    async def local_inputs(*_args):
        return _dependencies()["label"], _dependencies()["ndc"]

    async def clinical_inputs(test_mode=False):
        assert not test_mode
        return {}, None

    async def should_not_run(*_args):
        raise AssertionError("scan or index work started")

    evidence_cls = type("Evidence", (), {"evidence_id": object()})
    monkeypatch.setattr(drug_indications, "db", database)
    monkeypatch.setattr(drug_indications, "local_indication_dependencies", local_inputs)
    monkeypatch.setattr(drug_indications, "_rxnorm_ids_by_product", should_not_run)
    monkeypatch.setattr(drug_indications, "_load_official_condition_context", clinical_inputs)
    monkeypatch.setattr(drug_indications, "_scan_condition_evidence", should_not_run)
    monkeypatch.setattr(drug_indications, "_create_indexes", should_not_run)

    with pytest.raises(RuntimeError, match="Clinical reference identity is unavailable"):
        await drug_indications._build_evidence_stage(evidence_cls, "rx_data", "20260921", 100, 10, False, None)

    assert database.events == ["begin", "rollback"]


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("importer_id", "unknown"),
        ("local_lineage_id", "not-a-uuid"),
        ("local_generation", True),
        ("local_generation", -1),
        ("origin_generation", 0),
        ("origin_generation", 1 << 63),
        ("published_at", "invalid-date"),
        ("published_at", datetime.datetime(2026, 9, 21)),
        ("relation_oids", []),
        ("relation_oids", [True]),
        ("relation_oids", [1 << 32]),
        ("consumed_dependencies", {"ndc": {}}),
    ],
)
def test_persisted_authority_rejects_malformed_generation_and_relation_identity(field, invalid):
    authority_row = _AuthorityDatabase().first_rows[1]
    authority_row[field] = invalid
    with pytest.raises(RuntimeError, match="result publication"):
        authority.validate_result_publication_authority(authority_row)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "exhausted", "missing_update"])
async def test_publication_fails_closed_without_writable_authority(fault):
    database = _AuthorityDatabase()
    if fault == "exhausted":
        database.first_rows = [{**database.first_rows[1], "local_generation": (1 << 63) - 1}]
    else:
        database.first_rows[0 if fault == "missing" else 1] = None

    with pytest.raises(RuntimeError, match="authority is unavailable|generation is exhausted"):
        await authority.publish_local_result_generation(
            database, importer_id="label", schema="rx_data", consumed_dependencies={}
        )
    assert (database.update_parameters is not None) == (fault == "missing_update")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "relation_rows",
    [[], [{}], [{"relation_name": "label", "relation_oid": None}], [{"relation_name": "product", "relation_oid": 11}]],
)
async def test_dependency_capture_rejects_absent_or_misbound_catalog_relations(relation_rows):
    database = _IdentityDatabase()
    database.all_rows = [relation_rows]
    with pytest.raises(RuntimeError, match="relations are unavailable"):
        await authority.local_indication_dependencies(database, "rx_data")
    assert database.lock.endswith(" IN ACCESS SHARE MODE")


@pytest.mark.asyncio
async def test_clinical_context_keeps_consumed_identity_and_closes_its_connection(monkeypatch):
    dependency = _dependencies()["clinical-reference"]
    connection = SimpleNamespace(close=AsyncMock())
    read_rows = AsyncMock(
        return_value=(
            [
                {
                    "rxcui": "1",
                    "condition_system": "SYNTHETIC",
                    "condition_code": "C1",
                    "relationship": "may_treat",
                    "source_attribution": None,
                }
            ],
            [
                {
                    "condition_system": "SYNTHETIC",
                    "condition_code": "C1",
                    "term": " Example  condition ",
                    "term_type": "preferred",
                }
            ],
            dependency,
        )
    )
    monkeypatch.setattr(drug_indications.asyncpg, "connect", AsyncMock(return_value=connection))
    monkeypatch.setattr(drug_indications, "_read_clinical_rows", read_rows)
    monkeypatch.setenv("HLTHPRT_CLINICAL_DB_SCHEMA", "clinical_fixture")
    relationships, captured = await drug_indications._load_official_condition_context()
    assert captured == dependency
    assert relationships["1"][0]["terms"] == [{"term": "example condition", "term_type": "preferred"}]
    assert relationships["1"][0]["source_attribution"] == drug_indications.NLM_ATTRIBUTION
    read_rows.assert_awaited_once_with(connection, "clinical_fixture")
    connection.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("test_mode", [False, True])
async def test_clinical_read_failure_closes_connection_and_never_fabricates_identity(monkeypatch, test_mode):
    connection = SimpleNamespace(close=AsyncMock())
    monkeypatch.setattr(drug_indications.asyncpg, "connect", AsyncMock(return_value=connection))
    monkeypatch.setattr(
        drug_indications, "_read_clinical_rows", AsyncMock(side_effect=RuntimeError("synthetic read failure"))
    )
    monkeypatch.delenv("HLTHPRT_DRUG_INDICATIONS_ALLOW_EMPTY", raising=False)
    if test_mode:
        assert await drug_indications._fetch_clinical_rows(test_mode=True) == ([], [], None)
    else:
        with pytest.raises(RuntimeError, match="Clinical terminology lookup failed: synthetic read failure"):
            await drug_indications._fetch_clinical_rows()
    connection.close.assert_awaited_once()
