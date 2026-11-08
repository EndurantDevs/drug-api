"""Dependent producers reject absent, foreign and unsafe native publication custody."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.drug_snapshot_runtime import ndc_publication as proof


def _native_origin():
    table_receipt_dict = {
        "oid": 1,
        "row_count": 1,
        "size_bytes": 1,
        "sha256": "a" * 64,
        "encoding": "postgres-copy-csv-v1",
        "columns": [],
    }
    return {
        "format": "ndc-publication-v1",
        "run_id": "synthetic-run",
        "attempt_id": "synthetic-attempt",
        "complete": True,
        "published": True,
        "completed_at": "2026-01-01T00:00:00Z",
        "acquisition": {"complete": True, "source_records": 1, "partitions": [{"records": 1, "declared_records": 1}]},
        "counts": {"source_products": 1, "source_packages": 1, "product": 1, "package": 1, "batches": 1},
        "tables": {"product": deepcopy(table_receipt_dict), "package": deepcopy(table_receipt_dict)},
    }


@pytest.mark.parametrize(
    "path,replacement",
    [
        (("complete",), False),
        (("published",), False),
        (("format",), "foreign.v1"),
        (("counts", "product"), True),
        (("counts", "batches"), 0),
        (("acquisition", "complete"), False),
        (("acquisition", "source_records"), 2),
        (("tables", "product", "sha256"), "invalid"),
        (("tables", "product", "oid"), True),
        (("tables", "package", "row_count"), 2),
        (("foreign",), True),
        (("run_id",), ""),
        (("attempt_id",), "x" * 129),
        (("counts",), {}),
        (("acquisition", "partitions"), []),
        (("acquisition", "partitions"), [{"records": 1, "declared_records": 2}]),
        (("acquisition", "partitions"), [{"records": 0, "declared_records": 0}]),
    ],
)
def test_closed_native_dependency_origin_refuses_tamper(path, replacement):
    receipt_by_field = _native_origin()
    proof._require_complete_native_origin(receipt_by_field)
    destination = receipt_by_field
    for key in path[:-1]:
        destination = destination[key]
    destination[path[-1]] = replacement
    with pytest.raises(ValueError):
        proof._require_complete_native_origin(receipt_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("safe", [None, False, 1])
async def test_dependency_heap_requires_actual_native_catalog_before_scanning(safe):
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=safe))
    with pytest.raises(RuntimeError, match="native read catalog differs"):
        await proof._require_dependency_heap(driver, 1)
    driver.fetchval.assert_awaited_once()


@pytest.mark.asyncio
async def test_dependency_copy_cannot_exceed_its_declared_bound():
    digest = proof.NdcCsvDigest(max_bytes=1)
    await digest.write(b"a")
    with pytest.raises(RuntimeError, match="byte bound"):
        await digest.write(b"b")
    assert digest.size_bytes == 1


@pytest.mark.parametrize(
    "encoded", [None, "", "null", "[]", '"scalar"', '{"same":1,"same":2}', " " * (proof._MAX_EVIDENCE_BYTES + 1)]
)
def test_dependency_comment_refuses_missing_duplicate_or_unbounded_evidence(encoded):
    with pytest.raises(ValueError):
        proof._decode_evidence(encoded)
