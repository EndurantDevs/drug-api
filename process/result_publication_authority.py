"""Durable publication identity for transferable drug result tables."""

from __future__ import annotations

import datetime
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID

import asyncpg
from sqlalchemy import text

TABLE_NAME = "result_publication_authority"
DEPENDENCY_FORMAT = "drug-result-consumed-dependencies-v1"
RELATION_NAMES_BY_IMPORTER = {
    "label": ("label",),
    "drug-indications": ("drug_condition_evidence",),
}
DEPENDENCY_RELATIONS = {
    "label": ("label",),
    "ndc": ("product",),
    "clinical-reference": ("code_relationship", "code_catalog", "code_synonym"),
}
_KNOWN_RELATIONS = {
    name
    for names in (*RELATION_NAMES_BY_IMPORTER.values(), *DEPENDENCY_RELATIONS.values())
    for name in names
}
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_MAX_GENERATION = (1 << 63) - 1
_MAX_OID = (1 << 32) - 1


@dataclass(frozen=True)
class ServingGeneration:
    """Portable identity of the producer generation currently being served."""

    origin_lineage_id: str
    origin_generation: int
    published_at: datetime.datetime

    def as_dict(self) -> dict[str, Any]:
        """Return the portable generation identity as canonical fields."""

        return {
            "origin_lineage_id": self.origin_lineage_id,
            "origin_generation": self.origin_generation,
            "published_at": _timestamp_text(self.published_at),
        }


@dataclass(frozen=True)
class ResultPublicationAuthority:
    """Destination-local counter and exact serving relation identity."""

    importer_id: str
    local_lineage_id: str
    local_generation: int
    serving_generation: ServingGeneration | None
    relation_oids: tuple[int, ...] | None
    consumed_dependencies: dict[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        """Return local and portable authority fields for receipts."""

        return {
            "importer_id": self.importer_id,
            "local_lineage_id": self.local_lineage_id,
            "local_generation": self.local_generation,
            "serving_generation": None if self.serving_generation is None else self.serving_generation.as_dict(),
            "relation_oids": None if self.relation_oids is None else list(self.relation_oids),
            "consumed_dependencies": self.consumed_dependencies,
        }


def schema_name(value: object) -> str:
    """Validate a PostgreSQL schema identifier before interpolation."""

    normalized = str(value or "").strip()
    if not _IDENTIFIER.fullmatch(normalized) or len(normalized.encode()) > 63:
        raise ValueError("result publication schema is invalid")
    return normalized


def _importer_id(value: object) -> str:
    if not isinstance(value, str) or value not in RELATION_NAMES_BY_IMPORTER:
        raise ValueError("result publication importer is invalid")
    return value


def _uuid_text(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except (AttributeError, TypeError, ValueError):
        raise ValueError("result publication lineage is invalid") from None


def _timestamp(value: object) -> datetime.datetime:
    if isinstance(value, str):
        try:
            value = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("result publication time is invalid") from None
    if not isinstance(value, datetime.datetime) or value.tzinfo is None:
        raise ValueError("result publication time is invalid")
    return value.astimezone(datetime.UTC)


def _timestamp_text(value: datetime.datetime) -> str:
    return _timestamp(value).isoformat().replace("+00:00", "Z")


def validate_serving_generation(value: object) -> ServingGeneration:
    """Validate one complete portable origin generation."""

    if isinstance(value, ServingGeneration):
        return value
    if not isinstance(value, Mapping) or set(value) != {
        "origin_lineage_id",
        "origin_generation",
        "published_at",
    }:
        raise ValueError("serving generation is invalid")
    generation = value["origin_generation"]
    if type(generation) is not int or not 0 < generation <= _MAX_GENERATION:
        raise ValueError("serving generation is invalid")
    return ServingGeneration(
        _uuid_text(value["origin_lineage_id"]),
        generation,
        _timestamp(value["published_at"]),
    )


def _relation_oids(importer_id: str, value: object) -> tuple[int, ...]:
    expected = RELATION_NAMES_BY_IMPORTER[importer_id]
    if not isinstance(value, (list, tuple)) or len(value) != len(expected):
        raise ValueError("result publication relation identity is invalid")
    relation_oids = tuple(value)
    if any(type(oid) is not int or not 0 < oid <= _MAX_OID for oid in relation_oids) or len(
        set(relation_oids)
    ) != len(relation_oids):
        raise ValueError("result publication relation identity is invalid")
    return relation_oids


def _dependency_entry(key: str, value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) not in ({"relations"}, {"relations", "generation"}):
        raise ValueError("consumed dependency identity is invalid")
    expected_names = DEPENDENCY_RELATIONS[key]
    relations = value["relations"]
    if not isinstance(relations, (list, tuple)) or len(relations) != len(expected_names):
        raise ValueError("consumed dependency identity is invalid")
    normalized_relations = []
    for expected_name, relation in zip(expected_names, relations, strict=True):
        if not isinstance(relation, Mapping) or set(relation) != {"name", "oid"}:
            raise ValueError("consumed dependency identity is invalid")
        oid = relation["oid"]
        if relation["name"] != expected_name or type(oid) is not int or not 0 < oid <= _MAX_OID:
            raise ValueError("consumed dependency identity is invalid")
        normalized_relations.append({"name": expected_name, "oid": oid})
    dependency_dict: dict[str, Any] = {"relations": normalized_relations}
    if "generation" in value:
        if key != "label":
            raise ValueError("consumed dependency generation is unsupported")
        dependency_dict["generation"] = validate_serving_generation(value["generation"]).as_dict()
    return dependency_dict


def validate_consumed_dependencies(importer_id: str, value: object) -> dict[str, Any]:
    """Validate the strict, versioned identity map stored with a publication."""

    importer = _importer_id(importer_id)
    if importer == "label":
        if value != {}:
            raise ValueError("label consumed dependencies must be empty")
        return {}
    expected_keys = {"format", *DEPENDENCY_RELATIONS}
    if not isinstance(value, Mapping) or set(value) != expected_keys or value.get("format") != DEPENDENCY_FORMAT:
        raise ValueError("consumed dependencies are invalid")
    return {
        "format": DEPENDENCY_FORMAT,
        **{key: _dependency_entry(key, value[key]) for key in DEPENDENCY_RELATIONS},
    }


def dependency_entry(key: str, relation_rows: object, *, generation: object = None) -> dict[str, Any]:
    """Build one canonical dependency entry from ordered relation name/OID pairs."""

    if key not in DEPENDENCY_RELATIONS or not isinstance(relation_rows, (list, tuple)):
        raise ValueError("consumed dependency identity is invalid")
    dependency_dict: dict[str, Any] = {
        "relations": [{"name": row[0], "oid": row[1]} for row in relation_rows],
    }
    if generation is not None:
        dependency_dict["generation"] = generation
    return _dependency_entry(key, dependency_dict)


def indication_dependencies(
    label: Mapping[str, Any],
    ndc: Mapping[str, Any],
    clinical_reference: Mapping[str, Any],
) -> dict[str, Any]:
    """Combine the complete, ordered indication input identity map."""

    return validate_consumed_dependencies(
        "drug-indications",
        {
            "format": DEPENDENCY_FORMAT,
            "label": label,
            "ndc": ndc,
            "clinical-reference": clinical_reference,
        },
    )


def _row_mapping(row: object) -> Mapping[str, Any]:
    mapping = getattr(row, "_mapping", row)
    if isinstance(mapping, asyncpg.Record):
        return dict(mapping)
    if not isinstance(mapping, Mapping):
        raise RuntimeError("result publication authority is unavailable")
    return mapping


def validate_result_publication_authority(authority_row: object) -> ResultPublicationAuthority:
    """Validate one migration-installed authority row."""

    authority = _row_mapping(authority_row)
    try:
        importer = _importer_id(authority.get("importer_id"))
        local_lineage = _uuid_text(authority.get("local_lineage_id"))
    except ValueError as error:
        raise RuntimeError("result publication authority is invalid") from error
    local_generation = authority.get("local_generation")
    if type(local_generation) is not int or not 0 <= local_generation <= _MAX_GENERATION:
        raise RuntimeError("result publication local generation is invalid")
    serving_fields = (
        authority.get("origin_lineage_id"),
        authority.get("origin_generation"),
        authority.get("published_at"),
        authority.get("relation_oids"),
        authority.get("consumed_dependencies"),
    )
    if all(field is None for field in serving_fields):
        if local_generation != 0:
            raise RuntimeError("result publication generation-zero authority is invalid")
        return ResultPublicationAuthority(importer, local_lineage, local_generation, None, None, None)
    if local_generation == 0 or any(field is None for field in serving_fields):
        raise RuntimeError("result publication serving generation is incomplete")
    try:
        serving = validate_serving_generation(
            {
                "origin_lineage_id": authority["origin_lineage_id"],
                "origin_generation": authority["origin_generation"],
                "published_at": authority["published_at"],
            }
        )
        relation_oids = _relation_oids(importer, authority["relation_oids"])
        dependencies = validate_consumed_dependencies(importer, authority["consumed_dependencies"])
    except ValueError as error:
        raise RuntimeError("result publication serving generation is invalid") from error
    return ResultPublicationAuthority(importer, local_lineage, local_generation, serving, relation_oids, dependencies)


async def _first(database: Any, statement: Any, **params: Any) -> object | None:
    if hasattr(database, "first"):
        return await database.first(statement, **params)
    return (await database.execute(statement, params)).mappings().one_or_none()


async def _all(database: Any, statement: Any, **params: Any) -> list[object]:
    if hasattr(database, "all"):
        return list(await database.all(statement, **params))
    return list((await database.execute(statement, params)).all())


async def read_result_publication_authority(
    database: Any, *, importer_id: str, schema: str, lock: bool = False
) -> ResultPublicationAuthority:
    """Read and optionally lock one importer authority row."""

    importer = _importer_id(importer_id)
    schema = schema_name(schema)
    suffix = " FOR UPDATE" if lock else ""
    row = await _first(
        database,
        text(
            "SELECT importer_id, local_lineage_id, local_generation, origin_lineage_id, origin_generation, "
            f"published_at, relation_oids, consumed_dependencies FROM \"{schema}\".\"{TABLE_NAME}\" "
            "WHERE importer_id=:importer_id" + suffix
        ),
        importer_id=importer,
    )
    if row is None:
        raise RuntimeError("result publication authority is unavailable")
    return validate_result_publication_authority(row)


async def relation_identities(
    database: Any,
    schema: str,
    relation_names: tuple[str, ...],
) -> tuple[tuple[str, int], ...]:
    """Resolve fixed relation names to exact ordered PostgreSQL OIDs."""

    schema = schema_name(schema)
    if not relation_names or any(name not in _KNOWN_RELATIONS for name in relation_names):
        raise ValueError("result publication relation names are invalid")
    relation_rows = await _all(
        database,
        text(
            "SELECT relation_name, to_regclass(format('%I.%I', CAST(:schema AS text), relation_name))::oid::bigint "
            "AS relation_oid "
            "FROM unnest(CAST(:relation_names AS text[])) WITH ORDINALITY AS relations(relation_name, ordinal) "
            "ORDER BY ordinal"
        ),
        schema=schema,
        relation_names=list(relation_names),
    )
    try:
        identities = tuple(
            (str(_row_mapping(relation_row)["relation_name"]), int(_row_mapping(relation_row)["relation_oid"]))
            for relation_row in relation_rows
        )
    except (KeyError, TypeError, ValueError, RuntimeError):
        raise RuntimeError("result publication relations are unavailable") from None
    if tuple(name for name, _oid in identities) != relation_names or any(
        not 0 < oid <= _MAX_OID for _name, oid in identities
    ):
        raise RuntimeError("result publication relations are unavailable")
    return identities


async def local_indication_dependencies(database: Any, schema: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Lock and identify the exact local relations consumed by an indication scan."""

    schema = schema_name(schema)
    await database.status(f'LOCK TABLE "{schema}"."label", "{schema}"."product" IN ACCESS SHARE MODE')
    label_relations = await relation_identities(database, schema, DEPENDENCY_RELATIONS["label"])
    product_relations = await relation_identities(database, schema, DEPENDENCY_RELATIONS["ndc"])
    label_generation = None
    try:
        authority = await read_result_publication_authority(database, importer_id="label", schema=schema)
        if authority.serving_generation is not None and authority.relation_oids == tuple(
            oid for _name, oid in label_relations
        ):
            label_generation = authority.serving_generation
    except RuntimeError:
        label_generation = None
    return (
        dependency_entry("label", label_relations, generation=label_generation),
        dependency_entry("ndc", product_relations),
    )


async def publish_local_result_generation(
    database: Any, *, importer_id: str, schema: str, consumed_dependencies: Mapping[str, Any]
) -> ResultPublicationAuthority:
    """Advance authority inside the caller's ordinary publication transaction."""

    importer = _importer_id(importer_id)
    schema = schema_name(schema)
    dependencies = validate_consumed_dependencies(importer, consumed_dependencies)
    current = await read_result_publication_authority(database, importer_id=importer, schema=schema, lock=True)
    if current.local_generation >= _MAX_GENERATION:
        raise RuntimeError("result publication local generation is exhausted")
    identities = await relation_identities(database, schema, RELATION_NAMES_BY_IMPORTER[importer])
    next_generation = current.local_generation + 1
    updated = await _first(
        database,
        text(
            f'UPDATE "{schema}"."{TABLE_NAME}" SET local_generation=:next_generation, '
            "origin_lineage_id=local_lineage_id, origin_generation=:next_generation, "
            "published_at=clock_timestamp(), relation_oids=CAST(:relation_oids AS bigint[]), "
            "consumed_dependencies=CAST(:consumed_dependencies AS jsonb) WHERE importer_id=:importer_id "
            "RETURNING importer_id, local_lineage_id, local_generation, origin_lineage_id, origin_generation, "
            "published_at, relation_oids, consumed_dependencies"
        ),
        importer_id=importer,
        next_generation=next_generation,
        relation_oids=[oid for _name, oid in identities],
        consumed_dependencies=json.dumps(dependencies, separators=(",", ":"), sort_keys=True),
    )
    if updated is None:
        raise RuntimeError("result publication authority is unavailable")
    return validate_result_publication_authority(updated)


__all__ = [
    "DEPENDENCY_FORMAT",
    "DEPENDENCY_RELATIONS",
    "RELATION_NAMES_BY_IMPORTER",
    "ResultPublicationAuthority",
    "ServingGeneration",
    "dependency_entry",
    "indication_dependencies",
    "local_indication_dependencies",
    "publish_local_result_generation",
    "read_result_publication_authority",
    "relation_identities",
    "schema_name",
    "validate_consumed_dependencies",
    "validate_result_publication_authority",
    "validate_serving_generation",
]
