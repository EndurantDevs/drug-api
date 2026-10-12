"""Recording native-protocol fixtures; these do not model PostgreSQL execution."""

import hashlib
import json
from copy import deepcopy

from sqlalchemy.dialects import postgresql

from db.drug_snapshot_runtime.ndc import Package, Product

MODELS = {"product": Product, "package": Package}
TYPES = {
    "VARCHAR": ("varchar", "character varying", "default"),
    "TEXT": ("text", "text", "default"),
    "JSON": ("json", "json", None),
    "JSON[]": ("_json", "json[]", None),
    "VARCHAR[]": ("_varchar", "character varying[]", "default"),
    "BOOLEAN": ("bool", "boolean", None),
    "DATE": ("date", "date", None),
    "BIGINT": ("int8", "bigint", None),
}


class CatalogRow(dict):
    """Preserve both named access and ordered values returned by native records."""

    def __iter__(self):
        return iter(self.values())


def column_catalog(model):
    return [
        {
            "name": column.name,
            "type": TYPES[column.type.compile(dialect=postgresql.dialect())][1],
            "not_null": not column.nullable,
        }
        for column in model.__table__.columns
    ]


class RecordedDependencyCatalog:
    """Return fixed catalog rows and record every query, COPY and lock request."""

    def __init__(self, mode="heap", *, schema="synthetic"):
        self.mode = mode
        self.schema = schema
        self.transaction = True
        self.isolation = "repeatable read"
        self.read_only = "on"
        self.search_path = '"$user", public'
        self.savepoints = []
        self.deparse_error = None
        self.queries = []
        self.commands = []
        self.copies = []
        self.buffers = {name: (f"synthetic-{name}\n".encode(),) for name in MODELS}
        self.physical = {"product": 101, "package": 102}
        self.canonical = {
            name: oid + (100 if mode == "alias" else 200 if mode == "adopted" else 0)
            for name, oid in self.physical.items()
        }
        self.safe = True
        self.ownership = True
        self.completion = "COPY 1"
        self.kinds = {oid: "v" if mode == "alias" else "r" for oid in self.canonical.values()}
        self.native = self._native_publication()
        self.receipt = (
            self.native
            if mode != "adopted"
            else {
                "contract": "ndc.csv.v1",
                "source_publication": {"native_publication": self.native},
                "publication_state": "adopted",
                "tables": {name: {"relation_oid": oid} for name, oid in self.canonical.items()},
            }
        )
        self.comments = {oid: json.dumps(self.receipt) for oid in self.canonical.values()}
        self.column_rows = {
            oid: column_catalog(MODELS[name]) for name in MODELS for oid in (self.canonical[name], self.physical[name])
        }
        self.run = {
            "status": "succeeded",
            "phase_detail": "ndc import published",
            "finished_at": "complete",
            "metrics": json.dumps({"ndc_attempt_id": "synthetic-attempt", "ndc_publication": self.native}),
            "params": "{}",
        }
        self.projections = {oid: self._projection(name, oid) for name, oid in self.canonical.items()}

    def _native_publication(self):
        return {
            "format": "ndc-publication-v1",
            "run_id": "synthetic-run",
            "attempt_id": "synthetic-attempt",
            "complete": True,
            "published": True,
            "completed_at": "2026-01-01T00:00:00Z",
            "acquisition": {
                "complete": True,
                "source_records": 1,
                "partitions": [{"records": 1, "declared_records": 1}],
            },
            "counts": {"source_products": 1, "source_packages": 1, "product": 1, "package": 1, "batches": 1},
            "tables": {
                name: {
                    "oid": oid,
                    "row_count": 1,
                    "size_bytes": len(b"".join(self.buffers[name])),
                    "sha256": hashlib.sha256(b"".join(self.buffers[name])).hexdigest(),
                    "encoding": "postgres-copy-csv-v1",
                    "columns": column_catalog(MODELS[name]),
                }
                for name, oid in self.physical.items()
            },
        }

    def _projection(self, name, alias_oid):
        model = MODELS[name]
        rule_oid, row_oid, array_oid = alias_oid + 1000, alias_oid + 2000, alias_oid + 3000
        attributes = []
        for position, column in enumerate(model.__table__.columns, 1):
            type_name, _formatted, collation = TYPES[column.type.compile(dialect=postgresql.dialect())]
            attributes.append(
                CatalogRow(
                    attnum=position, attname=column.name, typname=type_name, atttypmod=-1, collname=collation, safe=True
                )
            )
        return {
            "attributes": attributes,
            "definition": "SELECT "
            + ", ".join(column.name for column in model.__table__.columns)
            + f" FROM {self.schema}.{name}_heap frozen;",
            "rules": [
                CatalogRow(oid=rule_oid, rulename="_RETURN", ev_type="1", ev_enabled="O", is_instead=True, ev_qual="<>")
            ],
            "rule_dependencies": [(0, "pg_class", alias_oid, 0, "i")]
            + [(0, "pg_class", self.physical[name], position, "n") for position in range(1, len(attributes) + 1)],
            "outgoing": [(0, "pg_namespace", 71, 0, "n")],
            "row_type": row_oid,
            "incoming": [("pg_type", row_oid, 0, 0, "i"), ("pg_rewrite", rule_oid, 0, 0, "i")],
            "type_dependents": [
                CatalogRow(
                    classid="pg_type",
                    objid=array_oid,
                    objsubid=0,
                    refobjsubid=0,
                    deptype="i",
                    typelem=row_oid,
                    typtype="b",
                )
            ],
            "array_dependents": False,
        }

    def is_in_transaction(self):
        return self.transaction

    def _name(self, identity):
        return next(name for name in MODELS if identity in (self.canonical[name], self.physical[name]))

    async def execute(self, statement):
        assert statement.startswith(
            ("LOCK TABLE ", "SET LOCAL ", "SAVEPOINT ", "ROLLBACK TO SAVEPOINT ", "RELEASE SAVEPOINT ")
        )
        self.commands.append(statement)
        if statement.startswith("SAVEPOINT "):
            self.savepoints.append((statement.removeprefix("SAVEPOINT "), self.search_path))
        elif statement.startswith("ROLLBACK TO SAVEPOINT "):
            name = statement.removeprefix("ROLLBACK TO SAVEPOINT ")
            assert self.savepoints[-1][0] == name
            self.search_path = self.savepoints[-1][1]
        elif statement.startswith("RELEASE SAVEPOINT "):
            assert self.savepoints[-1][0] == statement.removeprefix("RELEASE SAVEPOINT ")
            self.savepoints.pop()
        elif statement.startswith("SET LOCAL search_path = "):
            self.search_path = statement.removeprefix("SET LOCAL search_path = ")

    async def fetchval(self, statement, *arguments):
        self.queries.append((statement, arguments))
        if statement == "SHOW transaction_isolation":
            return self.isolation
        if statement == "SHOW transaction_read_only":
            return self.read_only
        if "to_regclass" in statement:
            return (
                99 if arguments[0].endswith(".import_run") else self.canonical[arguments[0].split(".")[-1].strip('"')]
            )
        if "obj_description" in statement:
            return self.comments[arguments[0]]
        if "SELECT relkind::text" in statement:
            return self.kinds[arguments[0]]
        if "SELECT c.relkind='r'" in statement:
            return self.safe
        if statement.startswith("SELECT count(*)"):
            return 1
        if "pg_get_viewdef" in statement:
            if self.deparse_error is not None:
                raise self.deparse_error
            definition = self.projections[arguments[0]]["definition"]
            if self.schema in {entry.strip() for entry in self.search_path.split(",")}:
                return definition.replace(f"FROM {self.schema}.", "FROM ")
            return definition
        if "array_agg(pg_catalog.quote_ident" in statement:
            return arguments[0]
        if "SELECT reltype" in statement:
            return self.projections[arguments[0]]["row_type"]
        if "SELECT EXISTS" in statement and "refclassid='pg_type'" in statement:
            return self.projections[arguments[0] - 3000]["array_dependents"]
        raise AssertionError("unexpected catalog scalar: " + statement)

    async def fetchrow(self, statement, *arguments):
        self.queries.append((statement, arguments))
        if "SELECT status,phase_detail" in statement:
            assert arguments == ("synthetic-run", 256 * 1024)
            return deepcopy(self.run)
        if "SELECT p.relname" in statement:
            name = self._name(arguments[0])
            assert arguments == (self.canonical[name], self.physical[name], self.schema)
            return (
                {"relation_name": name + "_heap", "schema_name": self.schema, "namespace_oid": 71}
                if self.ownership
                else None
            )
        raise AssertionError("unexpected catalog record: " + statement)

    async def fetch(self, statement, *arguments):
        self.queries.append((statement, arguments))
        if "SELECT attname AS name" in statement:
            return self.column_rows[arguments[0]]
        if "SELECT a.attnum" in statement:
            return self.projections[arguments[0]]["attributes"]
        if "FROM pg_catalog.pg_rewrite" in statement:
            return self.projections[arguments[0]]["rules"]
        if "classid='pg_rewrite'" in statement:
            return self.projections[arguments[0] - 1000]["rule_dependencies"]
        if "WHERE classid='pg_class'" in statement:
            return self.projections[arguments[0]]["outgoing"]
        if "WHERE refclassid='pg_class'" in statement:
            return self.projections[arguments[0]]["incoming"]
        if "WHERE d.refclassid='pg_type'" in statement:
            return self.projections[arguments[0] - 2000]["type_dependents"]
        raise AssertionError("unexpected catalog rows: " + statement)

    async def copy_from_query(self, statement, *, output, timeout, **copy_options):
        assert copy_options == {"format": "csv"}
        name = next(name for name in MODELS if f'FROM "{self.schema}"."{name}"' in statement)
        key = "product_id" if name == "product" else "package_ndc"
        assert statement.endswith(f'ORDER BY "{key}" COLLATE "C"') and timeout == 120
        self.copies.append(name)
        for buffer in self.buffers[name]:
            await output(buffer)
        return self.completion
