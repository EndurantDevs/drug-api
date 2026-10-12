"""Read-only native NDC audit shared by publication and dependent producers."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from types import SimpleNamespace

from sqlalchemy.dialects import postgresql

from . import projection_catalog
from .ndc import Package, Product

_MODELS = {"product": Product, "package": Package}
_MAX_EVIDENCE_BYTES = 256 * 1024
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")


def _quoted(value):
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError("NDC relation identity differs")
    return '"' + value + '"'


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


@dataclass
class NdcCsvDigest:
    """Hash awaited native COPY buffers without retaining row payloads."""

    digest: object = field(default_factory=hashlib.sha256)
    size_bytes: int = 0
    max_bytes: int = 32 * 1024**3

    async def write(self, chunk):
        """Count and hash the awaited native audit buffer within its byte cap."""
        if self.size_bytes + len(chunk) > self.max_bytes:
            raise RuntimeError("NDC native COPY audit exceeds its byte bound")
        self.digest.update(chunk)
        self.size_bytes += len(chunk)


async def audit_ndc_table(driver, *, schema, table_name, name, expected_oid, expected_count, _physical_oid=None):
    """Use the producer's genuine declarations and exact ordered native CSV encoding."""
    if name not in _MODELS:
        raise ValueError("NDC audit model differs")
    model = _MODELS[name]
    relation = _quoted(schema) + "." + _quoted(table_name)
    oid = await driver.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
    row_count = await driver.fetchval(f"SELECT count(*) FROM {relation}")
    if oid != expected_oid or row_count != expected_count:
        raise RuntimeError("NDC audit physical identity or census differs")
    columns = ", ".join(_quoted(column.name) for column in model.__table__.columns)
    primary_key = tuple(model.__table__.primary_key.columns)[0].name
    digest = NdcCsvDigest()
    completion = await driver.copy_from_query(
        f'SELECT {columns} FROM {relation} ORDER BY {_quoted(primary_key)} COLLATE "C"',
        output=digest.write,
        format="csv",
        timeout=120,
    )
    if completion != f"COPY {row_count}":
        raise RuntimeError("NDC audit COPY census differs")
    column_rows = await driver.fetch(
        "SELECT attname AS name,format_type(atttypid,atttypmod) AS type,attnotnull AS not_null "
        "FROM pg_attribute WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        oid if _physical_oid is None else _physical_oid,
    )
    return {
        "oid": oid if _physical_oid is None else _physical_oid,
        "row_count": row_count,
        "size_bytes": digest.size_bytes,
        "sha256": digest.digest.hexdigest(),
        "encoding": "postgres-copy-csv-v1",
        "columns": [dict(column_row) for column_row in column_rows],
    }


def _decode_evidence(encoded):
    if not isinstance(encoded, str) or not 0 < len(encoded.encode()) <= _MAX_EVIDENCE_BYTES:
        raise ValueError("NDC dependency evidence is missing or exceeds its bound")
    evidence = json.loads(encoded, object_pairs_hook=_unique_object)
    if type(evidence) is not dict:
        raise ValueError("NDC dependency evidence must be an object")
    return evidence


def _unique_object(pairs):
    object_by_name = {}
    for name, value in pairs:
        if name in object_by_name:
            raise ValueError("NDC dependency evidence repeats a field")
        object_by_name[name] = value
    return object_by_name


async def capture_ndc_dependency(driver, *, schema):
    """Pin one genuinely completed native pair; missing generations never mean no RxCUIs."""
    if (
        not driver.is_in_transaction()
        or await driver.fetchval("SHOW transaction_isolation") != "repeatable read"
        or await driver.fetchval("SHOW transaction_read_only") != "on"
    ):
        raise RuntimeError("NDC dependency requires an existing read-only repeatable-read transaction")
    quoted = _quoted(schema)
    await driver.execute(f"LOCK TABLE {quoted}.product,{quoted}.package IN ACCESS SHARE MODE NOWAIT")
    await driver.execute("SET LOCAL DateStyle TO 'ISO, YMD'")
    receipts = []
    physical_by_name = {}
    kinds = []
    for name in _MODELS:
        oid = await driver.fetchval("SELECT to_regclass($1)::oid::bigint", f"{quoted}.{_quoted(name)}")
        comment = await driver.fetchval(
            "SELECT CASE WHEN octet_length(obj_description($1,'pg_class'))<=$2 THEN obj_description($1,'pg_class') END",
            oid,
            _MAX_EVIDENCE_BYTES,
        )
        kinds.append(await driver.fetchval("SELECT relkind::text FROM pg_catalog.pg_class WHERE oid=$1", oid))
        receipts.append(_decode_evidence(comment))
        physical_by_name[name] = oid
    if _canonical(receipts[0]) != _canonical(receipts[1]):
        raise RuntimeError("NDC dependency paired publication differs")
    receipt = receipts[0]
    native = receipt
    if receipt.get("contract") == "ndc.csv.v1":
        source_publication = receipt.get("source_publication")
        if type(source_publication) is not dict:
            raise ValueError("NDC dependency source publication must be an object")
        native = source_publication.get("native_publication")
    _require_complete_native_origin(native)
    if kinds == ["v", "v"] and receipt is native:
        physical_by_name = await _require_dependency_aliases(driver, schema, physical_by_name, native)
    elif kinds == ["r", "r"]:
        for oid in physical_by_name.values():
            await _require_dependency_heap(driver, oid)
    else:
        raise RuntimeError("NDC dependency paired relation route differs")
    if receipt is native:
        await _require_completed_native_run(driver, quoted, native)
    elif receipt.get("publication_state") != "adopted":
        raise RuntimeError("NDC dependency adoption is incomplete")
    await _require_current_pair(driver, schema, physical_by_name, receipt, native, aliases=kinds == ["v", "v"])
    return {
        "contract": "ndc-native-dependency.v1",
        "native_publication_sha256": hashlib.sha256(_canonical(native)).hexdigest(),
        "product_sha256": native["tables"]["product"]["sha256"],
        "package_sha256": native["tables"]["package"]["sha256"],
    }


def _require_complete_native_origin(native):
    fields = {
        "format",
        "run_id",
        "attempt_id",
        "complete",
        "published",
        "completed_at",
        "acquisition",
        "counts",
        "tables",
    }
    if type(native) is not dict or set(native) != fields or len(_canonical(native)) > 128 * 1024:
        raise ValueError("NDC native dependency origin differs")
    if native["format"] != "ndc-publication-v1" or native["complete"] is not True or native["published"] is not True:
        raise ValueError("NDC native dependency is not a complete publication")
    for name in ("run_id", "attempt_id", "completed_at"):
        if not isinstance(native[name], str) or not 0 < len(native[name]) <= 128:
            raise ValueError("NDC native dependency attempt differs")
    _require_counts(native["counts"])
    acquisition = native["acquisition"]
    if (
        type(acquisition) is not dict
        or acquisition.get("complete") is not True
        or type(native["tables"]) is not dict
        or set(native["tables"]) != set(_MODELS)
    ):
        raise ValueError("NDC native dependency acquisition is incomplete")
    source_records = acquisition.get("source_records")
    if (
        type(source_records) is not int
        or source_records <= 0
        or source_records != native["counts"].get("source_products")
    ):
        raise ValueError("NDC native dependency acquisition census differs")
    partitions = acquisition.get("partitions")
    if not isinstance(partitions, list) or not 1 <= len(partitions) <= 64:
        raise ValueError("NDC native dependency partitions differ")
    if any(
        type(part) is not dict
        or type(part.get("records")) is not int
        or not 0 <= part["records"] <= 2**63 - 1
        or type(part.get("declared_records")) is not int
        or part["records"] != part["declared_records"]
        for part in partitions
    ):
        raise ValueError("NDC native dependency partition census differs")
    if sum(part["records"] for part in partitions) != source_records:
        raise ValueError("NDC native dependency source census differs")
    for name in _MODELS:
        _require_table_receipt(native["tables"][name], native["counts"][name])


def _require_counts(counts):
    if type(counts) is not dict or set(counts) != {
        "source_products",
        "source_packages",
        "product",
        "package",
        "batches",
    }:
        raise ValueError("NDC native dependency counts differ")
    if any(type(value) is not int or not 0 <= value <= 2**63 - 1 for value in counts.values()):
        raise ValueError("NDC native dependency count bounds differ")
    if (
        not 0 < counts["product"] <= counts["source_products"]
        or counts["package"] > counts["source_packages"]
        or counts["batches"] < 1
    ):
        raise ValueError("NDC native dependency census differs")


def _require_table_receipt(receipt, count):
    fields = {"oid", "row_count", "size_bytes", "sha256", "encoding", "columns"}
    if (
        type(receipt) is not dict
        or set(receipt) != fields
        or type(receipt["oid"]) is not int
        or not 0 < receipt["oid"] < 2**32
    ):
        raise ValueError("NDC native dependency table differs")
    if (
        type(receipt["row_count"]) is not int
        or receipt["row_count"] != count
        or type(receipt["size_bytes"]) is not int
        or not 0 <= receipt["size_bytes"] <= 32 * 1024**3
        or receipt["encoding"] != "postgres-copy-csv-v1"
        or not isinstance(receipt["sha256"], str)
        or re.fullmatch(r"[0-9a-f]{64}", receipt["sha256"]) is None
        or type(receipt["columns"]) is not list
    ):
        raise ValueError("NDC native dependency table content differs")


async def _require_dependency_heap(driver, oid):
    """Reject executable read hooks before scanning a claimed native dependency."""
    safe = await driver.fetchval(
        "SELECT c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition AND NOT c.relrowsecurity "
        "AND NOT c.relforcerowsecurity AND NOT c.relhasrules "
        "AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) "
        "AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) "
        "AND NOT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid "
        "WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
        "AND (a.attgenerated<>'' OR a.attidentity<>'' OR t.typnamespace<>'pg_catalog'::regnamespace)) "
        "FROM pg_class c WHERE c.oid=$1",
        oid,
    )
    if safe is not True:
        raise RuntimeError("NDC dependency native read catalog differs")


async def _require_completed_native_run(driver, schema, native):
    await driver.execute(f"LOCK TABLE {schema}.import_run IN ACCESS SHARE MODE NOWAIT")
    await _require_dependency_heap(
        driver, await driver.fetchval("SELECT pg_catalog.to_regclass($1)::oid", f"{schema}.import_run")
    )
    row = await driver.fetchrow(
        f"SELECT status,phase_detail,finished_at,metrics::text,params::text FROM {schema}.import_run "
        "WHERE importer='ndc' AND run_id=$1 AND octet_length(metrics::text)<=$2 AND octet_length(params::text)<=$2",
        native["run_id"],
        _MAX_EVIDENCE_BYTES,
    )
    if (
        row is None
        or row["status"] != "succeeded"
        or row["phase_detail"] != "ndc import published"
        or row["finished_at"] is None
    ):
        raise RuntimeError("NDC dependency run is not a completed native publication")
    metrics, params = _decode_evidence(row["metrics"]), _decode_evidence(row["params"])
    if params.get("test_mode") or params.get("test") or metrics.get("ndc_attempt_id") != native["attempt_id"]:
        raise RuntimeError("NDC dependency sample or attempt differs")
    if metrics.get("ndc_publication") != native:
        raise RuntimeError("NDC dependency run and serving receipt differ")


async def _require_current_pair(driver, schema, physical_by_name, receipt, native, *, aliases=False):
    adopted_tables = receipt.get("tables")
    if receipt is not native and (
        type(adopted_tables) is not dict or any(type(adopted_tables.get(name)) is not dict for name in _MODELS)
    ):
        raise ValueError("NDC dependency adopted table receipts must be objects")
    for name in _MODELS:
        expected = native["tables"][name]
        oid = physical_by_name[name]
        if receipt is native:
            if expected["oid"] != oid:
                raise RuntimeError("NDC dependency native OID differs")
        else:
            if adopted_tables[name].get("relation_oid") != oid:
                raise RuntimeError("NDC dependency adopted OID differs")
        canonical_oid = (
            (await driver.fetchval("SELECT pg_catalog.to_regclass($1)::oid", f"{_quoted(schema)}.{_quoted(name)}"))
            if aliases
            else oid
        )
        actual = await audit_ndc_table(
            driver,
            schema=schema,
            table_name=name,
            name=name,
            expected_oid=canonical_oid,
            expected_count=expected["row_count"],
            _physical_oid=oid if aliases else None,
        )
        if any(
            actual[field] != expected[field] for field in ("row_count", "size_bytes", "sha256", "encoding", "columns")
        ):
            raise RuntimeError("NDC dependency content or declaration differs")


def model_projection(schema, name):
    """Compile only the genuine model's fixed builtin projection declaration."""
    types_by_sql = {
        "VARCHAR": ("varchar", "default"),
        "TEXT": ("text", "default"),
        "JSON": ("json", None),
        "JSON[]": ("_json", None),
        "VARCHAR[]": ("_varchar", "default"),
        "BOOLEAN": ("bool", None),
        "DATE": ("date", None),
        "BIGINT": ("int8", None),
    }
    columns = []
    for column in _MODELS[name].__table__.columns:
        type_name, collation = types_by_sql[column.type.compile(dialect=postgresql.dialect())]
        columns.append(
            SimpleNamespace(name=column.name, type_name=type_name, type_modifier=-1, collation_name=collation)
        )
    return SimpleNamespace(schema_name=schema, relation_name=name, columns=tuple(columns))


async def _require_dependency_aliases(driver, schema, canonical_oids, native):
    """Verify inert protected aliases before COPY can expand their definitions."""
    physical_by_name = {}
    for name, alias_oid in canonical_oids.items():
        physical_oid = native["tables"][name]["oid"]
        await _require_dependency_heap(driver, physical_oid)
        physical = await _require_alias_ownership(driver, alias_oid, physical_oid, schema)
        alias = model_projection(schema, name)
        await projection_catalog.require_columns(driver, alias_oid, alias.columns)
        definition = await projection_catalog.view_definition(driver, alias_oid)
        projection_catalog.require(
            " ".join(definition.split()) == await projection_catalog.select_definition(driver, alias, physical),
            "definition_changed",
        )
        await projection_catalog.require_dependencies(
            driver, alias_oid, physical["namespace_oid"], physical_oid, len(alias.columns)
        )
        physical_by_name[name] = physical_oid
    return physical_by_name


async def _require_alias_ownership(driver, alias_oid, physical_oid, schema):
    """No reader membership or direct-write path may reach the protected pair."""
    physical = await driver.fetchrow(
        "SELECT p.relname AS relation_name,n.nspname AS schema_name,n.oid AS namespace_oid "
        "FROM pg_catalog.pg_class p JOIN pg_catalog.pg_class a ON a.oid=$1 "
        "JOIN pg_catalog.pg_namespace n ON n.oid=p.relnamespace "
        "JOIN pg_catalog.pg_roles owner ON owner.oid=p.relowner "
        "JOIN pg_catalog.pg_roles namespace_owner ON namespace_owner.oid=n.nspowner "
        "WHERE p.oid=$2 AND n.nspname=$3 AND a.relnamespace=n.oid AND a.relowner=p.relowner "
        "AND a.relkind='v' AND a.relpersistence='p' AND a.reloptions IS NULL "
        "AND NOT a.relispartition AND NOT a.relrowsecurity AND NOT a.relforcerowsecurity "
        "AND NOT (owner.rolcanlogin OR owner.rolsuper OR owner.rolcreaterole OR owner.rolcreatedb "
        "OR owner.rolreplication OR owner.rolbypassrls OR namespace_owner.rolcanlogin "
        "OR namespace_owner.rolsuper OR namespace_owner.rolcreaterole OR namespace_owner.rolcreatedb "
        "OR namespace_owner.rolreplication OR namespace_owner.rolbypassrls) "
        "AND NOT pg_catalog.pg_has_role(current_user,p.relowner,'MEMBER') "
        "AND NOT pg_catalog.pg_has_role(current_user,n.nspowner,'MEMBER') "
        "AND NOT pg_catalog.has_table_privilege(current_user,p.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER,MAINTAIN') "
        "AND NOT pg_catalog.has_any_column_privilege(current_user,p.oid,'INSERT,UPDATE') "
        "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=a.oid) "
        "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attrdef WHERE adrelid=a.oid) "
        "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid=a.oid) "
        "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=a.oid OR inhparent=a.oid) "
        "AND NOT EXISTS(SELECT 1 FROM pg_catalog.aclexplode(a.relacl) acl WHERE acl.grantee<>a.relowner "
        "AND (acl.grantee=0 OR acl.privilege_type<>'SELECT' OR acl.is_grantable))",
        alias_oid,
        physical_oid,
        schema,
    )
    if physical is None:
        raise RuntimeError("NDC dependency alias ownership differs")
    return physical
