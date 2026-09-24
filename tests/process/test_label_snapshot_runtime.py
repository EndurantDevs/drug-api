"""The application and archive worker share one Label column declaration."""

import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.drug_snapshot_runtime import publication
from db.drug_snapshot_runtime.label import Label as ArchiveLabel
from db.models import Label, db
from process.label import _label_row_dict_from_record


def test_label_model_preserves_application_metadata_and_full_columns():
    assert Label.__table__.metadata is db.Model.metadata
    assert tuple(Label.__table__.columns.keys()) == tuple(ArchiveLabel.__table__.columns.keys())
    assert len(Label.__table__.columns) > 100
    for application, archive in zip(Label.__table__.columns, ArchiveLabel.__table__.columns, strict=True):
        assert str(application.type) == str(archive.type)
        assert application.nullable == archive.nullable
        assert application.primary_key == archive.primary_key


def test_label_projection_keeps_nested_ndc_arrays_after_all_columns():
    columns = list(Label.__table__.columns.keys())
    label_record_dict = {
        "id": "synthetic-label",
        "openfda": {
            "product_ndc": ["00000-001"],
            "package_ndc": ["00000-001-01"],
        },
    }

    row = _label_row_dict_from_record(label_record_dict, columns)

    assert row["product_ndc"] == ["00000-001"]
    assert row["package_ndc"] == ["00000-001-01"]


def test_archive_model_and_publication_do_not_boot_an_application():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import db.drug_snapshot_runtime.label; import db.drug_snapshot_runtime.publication; assert 'sanic' not in sys.modules; assert 'db.connection' not in sys.modules",
        ],
        check=True,
    )


@pytest.mark.asyncio
async def test_adoption_binds_local_oid_and_preserves_foreign_origin():
    source_by_field = {
        "origin_lineage_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        "origin_generation": 7,
        "published_at": "2026-09-21T12:00:00Z",
    }
    initial_by_field = {
        "importer_id": "label",
        "local_lineage_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "local_generation": 0,
        "origin_lineage_id": None,
        "origin_generation": None,
        "published_at": None,
        "relation_oids": None,
        "consumed_dependencies": None,
    }
    updated_by_field = {
        **initial_by_field,
        **source_by_field,
        "local_generation": 1,
        "relation_oids": [79],
        "consumed_dependencies": {},
    }
    database = SimpleNamespace(
        first=AsyncMock(side_effect=[initial_by_field, updated_by_field]),
        all=AsyncMock(return_value=[{"relation_name": "label", "relation_oid": 79}]),
    )
    adopted_authority = await publication.adopt_label_generation(
        database, schema="rx_data", source_generation=source_by_field
    )
    assert adopted_authority.serving_generation.as_dict() == source_by_field
    assert adopted_authority.local_lineage_id == initial_by_field["local_lineage_id"]
    assert adopted_authority.local_generation == 1
    assert adopted_authority.relation_oids == (79,)
    assert adopted_authority.consumed_dependencies == {}
    assert database.first.await_args_list[1].kwargs["oids"] == [79]
    assert database.first.await_args_list[1].kwargs["lineage"] == source_by_field["origin_lineage_id"]


@pytest.mark.asyncio
async def test_adoption_rejects_missing_origin_and_counter_exhaustion(monkeypatch):
    database = SimpleNamespace(first=AsyncMock(), all=AsyncMock())
    with pytest.raises(ValueError):
        await publication.adopt_label_generation(database, schema="rx_data", source_generation=None)
    database.first.assert_not_awaited()
    monkeypatch.setattr(
        publication,
        "read_result_publication_authority",
        AsyncMock(return_value=SimpleNamespace(local_generation=(1 << 63) - 1)),
    )
    source_by_field = {
        "origin_lineage_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        "origin_generation": 7,
        "published_at": "2026-09-21T12:00:00Z",
    }
    with pytest.raises(RuntimeError, match="exhausted"):
        await publication.adopt_label_generation(database, schema="rx_data", source_generation=source_by_field)
    database.all.assert_not_awaited()
