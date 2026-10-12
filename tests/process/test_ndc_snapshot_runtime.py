"""Application and archive copying use the same complete product/package models."""

import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.drug_snapshot_runtime.ndc import RELATIONSHIPS
from db.drug_snapshot_runtime.ndc import Package as ArchivePackage
from db.drug_snapshot_runtime.ndc import Product as ArchiveProduct
from db.models import Package, Product, db
from process import ndc_publish


@pytest.mark.parametrize(
    ("application", "archive", "count", "key"),
    [
        (Product, ArchiveProduct, 23, "product_id"),
        (Package, ArchivePackage, 10, "package_ndc"),
    ],
)
def test_ndc_application_reuses_archive_columns_without_schema_changes(application, archive, count, key):
    assert application.__table__.metadata is db.Model.metadata
    assert tuple(application.__table__.columns.keys()) == tuple(archive.__table__.columns.keys())
    assert len(application.__table__.columns) == count
    assert tuple(application.__table__.primary_key.columns.keys()) == (key,)
    assert not application.__table__.foreign_keys
    assert not application.__table__.indexes
    for actual, expected in zip(application.__table__.columns, archive.__table__.columns, strict=True):
        assert str(actual.type) == str(expected.type)
        assert actual.nullable == expected.nullable
        assert actual.primary_key == expected.primary_key
        assert actual.server_default == expected.server_default
    assert application.query is not None


def test_ndc_runtime_does_not_initialize_an_application():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from db.drug_snapshot_runtime.ndc import Product, Package; "
            "assert 'sanic' not in sys.modules; assert 'db.connection' not in sys.modules; "
            "assert Product.__table__.metadata is Package.__table__.metadata",
        ],
        check=True,
    )


def test_package_edge_preserves_nonunique_product_ndc_semantics():
    assert RELATIONSHIPS == ((ArchivePackage, "product_ndc", ArchiveProduct, "product_ndc", False, False),)
    assert not ArchiveProduct.__table__.c.product_ndc.primary_key
    assert not ArchiveProduct.__table__.c.product_ndc.unique
    assert ArchiveProduct.__my_additional_indexes__[0]["index_elements"] == ["product_ndc"]
    assert ArchiveProduct.__my_additional_indexes__[0]["using"] == "gin"


@pytest.mark.asyncio
async def test_ordinary_product_indexes_use_archive_declarations_with_legacy_names():
    database = SimpleNamespace(status=AsyncMock())
    await ndc_publish._create_product_indexes(database, "synthetic", "incoming")
    assert [call.args[0] for call in database.status.await_args_list] == [
        "CREATE INDEX product_idx_brand_trgm_idx_incoming ON synthetic.product_incoming USING GIN(brand_name gin_trgm_ops);",
        "CREATE INDEX product_idx_generic_trgm_idx_incoming ON synthetic.product_incoming USING GIN(generic_name gin_trgm_ops);",
        "CREATE INDEX product_rxnorm_idx_incoming ON synthetic.product_incoming USING GIN(rxnorm_ids);",
    ]
