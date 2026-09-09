"""Controlled HTTP sources and isolated connection fixtures for native publication."""

import io
import json
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.models import Package, Product
from process import ndc_acquire
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
                patch.setattr(database, "session_factory", async_sessionmaker(
                    bind=connection, expire_on_commit=False, autoflush=False,
                ))
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
            output=collect, format="csv", timeout=5,
        )
        assert completion == "COPY 1"
        payloads_by_table[model.__tablename__] = b"".join(chunks)
    return payloads_by_table


def install_coordinator_sources(monkeypatch, manifest_url):
    """Mock only HTTP transport and observe the real temporary-directory lifecycle."""
    section_dict, payloads_by_url = _coordinator_sources(manifest_url)
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


def _coordinator_sources(manifest_url):
    partitions = [{"file": f"https://example.test/ndc/part-{ordinal}.json.zip", "records": 1} for ordinal in (1, 2)]
    section_dict = {"export_date": "2026-01-01", "total_records": 2, "partitions": partitions}
    payloads_by_url = {manifest_url: json.dumps({"results": {"drug": {"ndc": section_dict}}}).encode()}
    for ordinal, partition in enumerate(partitions, start=1):
        record_dict = {"product_id": f"synthetic-source-{ordinal}", "product_ndc": f"90000-000{ordinal}",
                       "dosage_form": "TABLET, FILM COATED", "product_type": "HUMAN PRESCRIPTION DRUG",
                       "generic_name": "Synthetic generic", "openfda": {"rxcui": [str(ordinal)]},
                       "marketing_start_date": "20200101", "packaging": [{
                           "package_ndc": f"90000-000{ordinal}-01",
                           "description": f"30 TABLET in 1 BOTTLE (90000-000{ordinal}-01)",
                       }]}
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("records.json", json.dumps({"results": [record_dict]}))
        payloads_by_url[partition["file"]] = buffer.getvalue()
    return section_dict, payloads_by_url
