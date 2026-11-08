"""Exercise read-only dependency custody using recorded native protocol replies."""

import asyncio
import hashlib
import json
from copy import deepcopy

import pytest

from db.drug_snapshot_runtime import ndc_publication as proof
from db.drug_snapshot_runtime import projection_catalog
from tests.process.ndc_catalog_fixtures import MODELS, RecordedDependencyCatalog


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["heap", "alias", "adopted", "adopted-alias"])
async def test_complete_pair_is_scanned_only_after_all_catalog_and_publication_checks(mode):
    driver = RecordedDependencyCatalog(mode)
    observed = await proof.capture_ndc_dependency(driver, schema="synthetic")
    expected_digest = hashlib.sha256(
        json.dumps(driver.native, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
    ).hexdigest()
    assert observed == {
        "contract": "ndc-native-dependency.v1",
        "native_publication_sha256": expected_digest,
        "product_sha256": driver.native["tables"]["product"]["sha256"],
        "package_sha256": driver.native["tables"]["package"]["sha256"],
    }
    assert driver.copies == ["product", "package"]
    assert driver.commands[:2] == [
        'LOCK TABLE "synthetic".product,"synthetic".package IN ACCESS SHARE MODE NOWAIT',
        "SET LOCAL DateStyle TO 'ISO, YMD'",
    ]
    assert any(".import_run " in statement for statement in driver.commands) == (
        mode not in {"adopted", "adopted-alias"}
    )
    if mode in {"alias", "adopted-alias"}:
        ownership_checks = [statement for statement, _args in driver.queries if "SELECT p.relname" in statement]
        assert len(ownership_checks) == 2
        assert all("pg_has_role(current_user,p.relowner,'MEMBER')" in statement for statement in ownership_checks)
        assert all("INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER,MAINTAIN" in statement for statement in ownership_checks)
        attribute_reads = [args[0] for statement, args in driver.queries if "SELECT attname AS name" in statement]
        assert attribute_reads == list(driver.physical.values())
    index_reads = [args[0] for statement, args in driver.queries if "SELECT am.amname AS method" in statement]
    assert index_reads == (list(driver.physical.values()) if mode == "adopted-alias" else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["transaction", "isolation", "read_only"])
async def test_dependency_scan_never_owns_or_downgrades_the_read_transaction(changed):
    driver = RecordedDependencyCatalog()
    setattr(driver, changed, False if changed == "transaction" else "invalid")
    with pytest.raises(RuntimeError, match="read-only repeatable-read"):
        await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert not driver.copies and not driver.commands


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed,reason",
    [
        ("pair", "paired publication"),
        ("mixed", "paired relation route"),
        ("view-adoption", "adopted view custody"),
        ("adoption", "adoption is incomplete"),
        ("native-oid", "native OID"),
        ("adopted-oid", "adopted OID"),
        ("run", "completed native publication"),
        ("phase", "completed native publication"),
        ("test-mode", "sample or attempt"),
        ("attempt", "sample or attempt"),
        ("run-receipt", "serving receipt"),
    ],
)
async def test_dependency_refuses_incomplete_or_replaced_authority_before_copy(changed, reason):
    driver = RecordedDependencyCatalog("adopted" if changed in {"adoption", "adopted-oid", "view-adoption"} else "heap")
    match changed:
        case "pair":
            driver.comments[driver.canonical["package"]] = json.dumps({"changed": True})
        case "mixed":
            driver.kinds[driver.canonical["package"]] = "v"
        case "view-adoption":
            driver.kinds = dict.fromkeys(driver.canonical.values(), "v")
        case "adoption" | "adopted-oid" | "native-oid":
            if changed == "adoption":
                driver.receipt["publication_state"] = "staged"
            elif changed == "adopted-oid":
                driver.receipt["tables"]["product"]["relation_oid"] = 999
            else:
                driver.native["tables"]["product"]["oid"] = 999
                driver.run["metrics"] = json.dumps(
                    {"ndc_attempt_id": "synthetic-attempt", "ndc_publication": driver.native}
                )
            driver.comments = dict.fromkeys(driver.canonical.values(), json.dumps(driver.receipt))
        case "run":
            driver.run = None
        case "phase":
            driver.run["phase_detail"] = "loading"
        case "test-mode":
            driver.run["params"] = '{"test_mode":true}'
        case "attempt":
            driver.run["metrics"] = json.dumps({"ndc_attempt_id": "other", "ndc_publication": driver.native})
        case _:
            driver.run["metrics"] = json.dumps({"ndc_attempt_id": "synthetic-attempt", "ndc_publication": {}})
    with pytest.raises((RuntimeError, ValueError), match=reason):
        await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert not driver.copies


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["heap", "adopted-alias"])
@pytest.mark.parametrize(
    "changed,reason",
    [("content", "content or declaration"), ("columns", "content or declaration"), ("copy-count", "COPY census")],
)
async def test_real_audit_recomputes_content_columns_and_copy_accounting(changed, reason, mode):
    driver = RecordedDependencyCatalog(mode)
    if changed == "content":
        driver.buffers["product"] = (b"replacement-data\n",)
    elif changed == "columns":
        driver.column_rows[driver.physical["product"]][0]["not_null"] = True
    else:
        driver.completion = "COPY 0"
    with pytest.raises(RuntimeError, match=reason):
        await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert driver.copies == ["product"]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, [], "invalid"])
@pytest.mark.parametrize(
    "field", ["comment", "metrics", "params", "source_publication", "tables", "product", "package"]
)
async def test_dependency_refuses_non_object_evidence_before_copy(field, value):
    driver = RecordedDependencyCatalog("adopted" if field in {"source_publication", "tables", *MODELS} else "heap")
    if field == "comment":
        driver.comments = dict.fromkeys(driver.canonical.values(), json.dumps(value))
    elif field in {"metrics", "params"}:
        driver.run[field] = json.dumps(value)
    else:
        if field in MODELS:
            driver.receipt["tables"][field] = value
        else:
            driver.receipt[field] = value
        driver.comments = dict.fromkeys(driver.canonical.values(), json.dumps(driver.receipt))
    with pytest.raises(ValueError, match="object"):
        await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert not driver.copies


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "schema,search_path",
    [("synthetic", "synthetic, public"), ("synthetic", "pg_catalog"), ("public", '"$user", public')],
)
@pytest.mark.parametrize("mode", ["alias", "adopted-alias"])
async def test_alias_deparsing_is_scoped_and_restores_the_callers_search_path(schema, search_path, mode):
    driver = RecordedDependencyCatalog(mode, schema=schema)
    driver.search_path = search_path
    driver.savepoints = [("ndc_projection_definition", "caller-owned")]
    await proof.capture_ndc_dependency(driver, schema=schema)
    assert driver.search_path == search_path
    assert driver.savepoints == [("ndc_projection_definition", "caller-owned")]
    assert driver.transaction and driver.isolation == "repeatable read" and driver.read_only == "on"
    assert driver.copies == ["product", "package"]
    assert driver.commands.count("SAVEPOINT ndc_projection_definition") == 2
    assert driver.commands.count("SET LOCAL search_path = pg_catalog, pg_temp") == 2
    assert driver.commands.count("ROLLBACK TO SAVEPOINT ndc_projection_definition") == 2
    assert driver.commands.count("RELEASE SAVEPOINT ndc_projection_definition") == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("native query failed"), asyncio.CancelledError()])
@pytest.mark.parametrize("mode", ["alias", "adopted-alias"])
async def test_alias_deparsing_rolls_back_its_setting_on_native_failure_or_cancellation(error, mode):
    driver = RecordedDependencyCatalog(mode)
    driver.search_path = "synthetic, public"
    driver.deparse_error = error
    with pytest.raises(type(error)) as failure:
        await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert failure.value is error
    assert driver.search_path == "synthetic, public" and not driver.savepoints
    assert driver.commands[-2:] == [
        "ROLLBACK TO SAVEPOINT ndc_projection_definition",
        "RELEASE SAVEPOINT ndc_projection_definition",
    ]
    assert driver.transaction and driver.isolation == "repeatable read" and driver.read_only == "on"
    assert not driver.copies


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["alias", "adopted-alias"])
@pytest.mark.parametrize(
    "changed,reason",
    [
        ("ownership", "ownership"),
        ("definition", "definition_changed"),
        ("columns", "columns_changed"),
        ("column-order", "columns_changed"),
        ("rules", "rules_changed"),
        ("rule-dependency", "dependencies_changed"),
        ("outgoing", "dependencies_changed"),
        ("incoming", "dependents_unsupported"),
        ("row-type", "dependents_unsupported"),
        ("array-type", "dependents_unsupported"),
    ],
)
async def test_inert_alias_catalog_is_checked_before_the_first_view_expansion(changed, reason, mode):
    driver = RecordedDependencyCatalog(mode)
    catalog = driver.projections[driver.canonical["product"]]
    match changed:
        case "ownership":
            driver.ownership = False
        case "definition":
            catalog["definition"] = "SELECT unreviewed_function();"
        case "columns":
            catalog["attributes"].pop()
        case "column-order":
            catalog["attributes"].reverse()
        case "rules":
            catalog["rules"][0]["ev_enabled"] = "D"
        case "rule-dependency":
            catalog["rule_dependencies"].append((0, "pg_proc", 999, 0, "n"))
        case "outgoing":
            catalog["outgoing"] = [(0, "pg_namespace", 999, 0, "n")]
        case "incoming":
            catalog["incoming"].append(("pg_rewrite", 999, 0, 0, "n"))
        case "row-type":
            catalog["type_dependents"][0]["typelem"] = 999
        case _:
            catalog["array_dependents"] = True
    with pytest.raises((RuntimeError, projection_catalog.ProjectionCatalogError), match=reason):
        await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert not driver.copies


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["identifier", "model", "oid", "count"])
async def test_audit_refuses_a_foreign_model_or_relation_before_native_copy(changed):
    driver = RecordedDependencyCatalog()
    audit_options_by_field = {
        "schema": "synthetic",
        "table_name": "product",
        "name": "product",
        "expected_oid": 101,
        "expected_count": 1,
    }
    audit_options_by_field[
        {"identifier": "schema", "model": "name", "oid": "expected_oid", "count": "expected_count"}[changed]
    ] = "foreign;sql" if changed in {"identifier", "model"} else 999
    with pytest.raises((ValueError, RuntimeError), match="identity|model|census"):
        await proof.audit_ndc_table(driver, **audit_options_by_field)
    assert not driver.copies


def test_projection_columns_are_exactly_the_real_product_and_package_declarations():
    for name, model in MODELS.items():
        declaration = proof.model_projection("synthetic", name)
        assert (declaration.schema_name, declaration.relation_name) == ("synthetic", name)
        assert tuple(column.name for column in declaration.columns) == tuple(model.__table__.columns.keys())
        assert all(column.type_modifier == -1 for column in declaration.columns)


@pytest.mark.asyncio
async def test_adopted_views_keep_original_receipt_without_local_native_run():
    driver = RecordedDependencyCatalog("adopted-alias")
    original = deepcopy(driver.native)
    driver.run = None
    observed = await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert driver.native == original
    assert observed["native_publication_sha256"] == hashlib.sha256(proof._canonical(original)).hexdigest()
    assert driver.physical == {"product": 401, "package": 402}
    assert [original["tables"][name]["oid"] for name in MODELS] == [101, 102]
    assert not any(".import_run" in statement for statement, _arguments in driver.queries)
    ownership_arguments = [arguments for statement, arguments in driver.queries if "SELECT p.relname" in statement]
    assert ownership_arguments == [(201, 401, "synthetic"), (202, 402, "synthetic")]
    assert driver.copies == ["product", "package"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("relation_oid", None),
        ("relation_oid", True),
        ("relation_oid", 0),
        ("relation_oid", 2**32),
        ("row_count", True),
        ("row_count", 2),
        ("size_bytes", True),
        ("size_bytes", 2),
        ("sha256", "0" * 64),
        ("columns", []),
    ],
)
async def test_adopted_view_receipt_binds_local_oid_and_native_payload(field, value):
    driver = RecordedDependencyCatalog("adopted-alias")
    driver.receipt["tables"]["product"][field] = value
    driver.comments = dict.fromkeys(driver.canonical.values(), json.dumps(driver.receipt))
    with pytest.raises(ValueError, match="adopted view custody"):
        await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert not driver.copies


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["duplicate", "foreign-oid", "alias-oid", "pair", "state", "heap"])
async def test_adopted_view_pair_cannot_borrow_another_local_binding(changed):
    driver = RecordedDependencyCatalog("adopted-alias")
    if changed == "duplicate":
        driver.receipt["tables"]["product"]["relation_oid"] = 402
    elif changed in {"foreign-oid", "alias-oid"}:
        driver.receipt["tables"]["product"]["relation_oid"] = 999 if changed == "foreign-oid" else 201
    elif changed == "pair":
        driver.receipt["tables"]["foreign"] = driver.receipt["tables"].pop("package")
    elif changed == "state":
        driver.receipt["publication_state"] = "staged"
    else:
        driver.safe = False
    driver.comments = dict.fromkeys(driver.canonical.values(), json.dumps(driver.receipt))
    with pytest.raises((ValueError, RuntimeError), match="custody|pair|ownership|read catalog"):
        await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert not driver.copies


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        "missing",
        "duplicate",
        "column_name",
        "method",
        "opclass",
        "provider",
        "collation",
        "is_primary",
        "is_unique",
        "is_safe",
    ],
)
async def test_installed_view_indexes_are_complete_exact_and_trusted_before_copy(changed):
    driver = RecordedDependencyCatalog("adopted-alias")
    indexes = driver.indexes_by_oid[401]
    if changed == "missing":
        indexes.pop()
    elif changed == "duplicate":
        indexes.append(deepcopy(indexes[0]))
    else:
        indexes[0][changed] = False if changed in {"is_primary", "is_unique", "is_safe"} else "foreign"
    with pytest.raises(RuntimeError, match="indexes differ"):
        await proof.capture_ndc_dependency(driver, schema="synthetic")
    assert not driver.copies


def test_installed_index_query_closes_code_and_native_state():
    for boundary in (
        "i.indexprs IS NULL AND i.indpred IS NULL",
        "i.indisvalid AND i.indisready AND i.indislive",
        "i.indnatts=1 AND i.indnkeyatts=1 AND i.indoption[0]=0",
        "ic.relowner=heap.relowner",
        "opc.opcmethod=ic.relam AND family.opfmethod=ic.relam",
        "pg_catalog.pg_has_role(current_user,opc.opcowner,'MEMBER')",
        "pg_catalog.pg_has_role(current_user,family.opfowner,'MEMBER')",
        "pg_catalog.pg_has_role(current_user,e.extowner,'MEMBER')",
        "pg_catalog.pg_has_role(current_user,p.proowner,'MEMBER')",
        "pg_catalog.pg_has_role(current_user,op.oprowner,'MEMBER')",
        "e.extname IN ('pg_trgm','btree_gin')",
        "WHERE i.indrelid=$1 LIMIT 8",
    ):
        assert boundary in proof._INSTALLED_INDEX_SQL
