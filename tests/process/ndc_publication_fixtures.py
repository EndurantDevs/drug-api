"""Controlled HTTP sources and isolated connection fixtures for native publication."""

import argparse
import asyncio
import io
import json
import os
import sys
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.connection import db
from db.models import Package, Product
from process import ndc_acquire, ndc_product
from process.ext import utils


@asynccontextmanager
async def non_iso_publication_connection(database, monkeypatch):
    """Pin one backend to a non-ISO session default and restore it before release."""
    async with database.engine.connect() as connection:
        initial_style = await connection.scalar(text("SHOW DateStyle"))
        await connection.rollback()
        try:
            await connection.execute(text("SET DateStyle TO 'SQL, DMY'"))
            await connection.commit()
            with monkeypatch.context() as patch:
                patch.setattr(
                    database,
                    "session_factory",
                    async_sessionmaker(
                        bind=connection,
                        expire_on_commit=False,
                        autoflush=False,
                    ),
                )
                yield connection
        finally:
            await connection.rollback()
            await connection.execute(text("SELECT set_config('DateStyle', :style, false)"), {"style": initial_style})
            await connection.commit()


async def copy_publication_pair_bytes(connection, schema):
    """Read the real dated pair with the current backend's COPY representation."""
    raw_connection = await connection.get_raw_connection()
    payloads_by_table = {}
    for model in (Product, Package):
        chunks = []

        async def collect(chunk):
            chunks.append(bytes(chunk))

        columns = ", ".join('"' + column.name + '"' for column in model.__table__.columns)
        primary_key = "product_id" if model is Product else "package_ndc"
        completion = await raw_connection.driver_connection.copy_from_query(
            f'SELECT {columns} FROM {schema}.{model.__tablename__} ORDER BY "{primary_key}" COLLATE "C"',
            output=collect,
            format="csv",
            timeout=5,
        )
        assert completion == "COPY 1"
        payloads_by_table[model.__tablename__] = b"".join(chunks)
    return payloads_by_table


def install_coordinator_sources(monkeypatch, manifest_url, *, brand_name=None):
    """Mock only HTTP transport and observe the real temporary-directory lifecycle."""
    section_dict, payloads_by_url = _coordinator_sources(manifest_url, brand_name=brand_name)
    temporary_paths = []
    temporary_directory = ndc_acquire.tempfile.TemporaryDirectory

    def observe_directory(*args, **kwargs):
        directory = temporary_directory(*args, **kwargs)
        temporary_paths.append(Path(directory.name))
        return directory

    def transport(**_kwargs):
        return httpx.MockTransport(lambda request: httpx.Response(200, content=payloads_by_url[str(request.url)]))

    monkeypatch.setattr(utils.httpx, "AsyncHTTPTransport", transport)
    monkeypatch.setattr(ndc_acquire.tempfile, "TemporaryDirectory", observe_directory)
    return section_dict, payloads_by_url, temporary_paths


def _coordinator_sources(manifest_url, *, brand_name=None):
    partitions = [{"file": f"https://example.test/ndc/part-{ordinal}.json.zip", "records": 1} for ordinal in (1, 2)]
    section_dict = {"export_date": "2026-01-01", "total_records": 2, "partitions": partitions}
    payloads_by_url = {manifest_url: json.dumps({"results": {"drug": {"ndc": section_dict}}}).encode()}
    for ordinal, partition in enumerate(partitions, start=1):
        record_dict = {
            "product_id": f"synthetic-source-{ordinal}",
            "product_ndc": f"90000-000{ordinal}",
            "dosage_form": "TABLET, FILM COATED",
            "product_type": "HUMAN PRESCRIPTION DRUG",
            "generic_name": "Synthetic generic",
            "openfda": {"rxcui": [str(ordinal)]},
            "marketing_start_date": "20200101",
            "packaging": [
                {
                    "package_ndc": f"90000-000{ordinal}-01",
                    "description": f"30 TABLET in 1 BOTTLE (90000-000{ordinal}-01)",
                }
            ],
        }
        if brand_name is not None:
            record_dict["brand_name"] = brand_name
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("records.json", json.dumps({"results": [record_dict]}))
        payloads_by_url[partition["file"]] = buffer.getvalue()
    return section_dict, payloads_by_url


def _producer_arguments(argv=None):
    """Bound the test coordinator's standalone identity and synthetic source label."""
    parser = argparse.ArgumentParser(description="Run one disposable NDC producer with controlled HTTP sources.")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--brand-name", required=True)
    arguments = parser.parse_args(argv)
    try:
        if str(UUID(arguments.job_id)) != arguments.job_id:
            raise ValueError("noncanonical UUID")
    except ValueError:
        parser.error("job-id must be a canonical UUID")
    label = arguments.brand_name
    if not 1 <= len(label) <= 64 or not label.isascii() or not label.isprintable() or label != label.strip():
        parser.error("brand-name must be a nonempty printable ASCII label of at most 64 characters")
    return arguments


def _require_local_producer_modules():
    """Reject database or producer modules imported from another source checkout."""
    root = Path(__file__).resolve().parents[2]
    for name, module in tuple(sys.modules.items()):
        if name in {"db", "process"} or name.startswith(("db.", "process.")):
            source = getattr(module, "__file__", None)
            locations = [source] if source is not None else list(getattr(module, "__path__", ()))
            assert locations and all(Path(path).resolve().is_relative_to(root) for path in locations), (
                "foreign producer module"
            )
    assert ndc_product.db is db, "producer database identity differs"


async def _run_coordinator(job_id, brand_name):
    """Use direct test-role login and the real coordinator; only source HTTP is mocked."""
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    assert os.getenv("HLTHPRT_ENVIRONMENT") == "test" and ("test" in database_name or database_name.endswith("_ci")), (
        "requires an explicitly configured disposable PostgreSQL test database"
    )
    assert os.getenv("DB_SCHEMA") == "rx_data" and os.getenv("HLTHPRT_NDC_PUBLICATION_MODE") == "handoff"
    assert os.getenv("HLTHPRT_IMPORT_NODE_ID"), "handoff node must be configured"
    builder_role = os.environ["HLTHPRT_DB_USER"]
    assert builder_role, "direct builder login must be configured"
    _require_local_producer_modules()
    temporary_paths = []
    try:
        await db.connect()
        identity = await db.first("SELECT current_user AS active_role, session_user AS login_role")
        assert identity["active_role"] == identity["login_role"] == builder_role, "builder login identity differs"
        with pytest.MonkeyPatch.context() as patch:
            manifest_url = "https://example.test/ndc/manifest.json"
            patch.setenv("HLTHPRT_MAIN_RX_JSON_URL", manifest_url)
            _, _, temporary_paths = install_coordinator_sources(patch, manifest_url, brand_name=brand_name)
            receipt = await ndc_product.init_file({"job_id": job_id}, {})
        _require_local_producer_modules()
        assert len(temporary_paths) == 3, "coordinator source directory census differs"
        return receipt
    finally:
        try:
            await db.disconnect()
        finally:
            assert all(not path.exists() for path in temporary_paths), "coordinator source directories remain"


def main(argv=None):
    """Emit only the genuine returned receipt after coordinator and connection cleanup."""
    arguments = _producer_arguments(argv)
    receipt = asyncio.run(_run_coordinator(arguments.job_id, arguments.brand_name))
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
