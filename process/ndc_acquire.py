"""Acquire exact FDA NDC inputs and await every parsed batch acknowledgment."""

import asyncio
import datetime
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path

import ijson
from aiofile import async_open
from async_unzip import unzip

from process.ext.utils import download_it_and_save

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_ARCHIVE_BYTES = 32 * 1024 * 1024 * 1024
MAX_PARTITIONS = 64


async def acquire_ndc_manifest(url: str, *, sample: bool, max_records: int) -> dict:
    """Authenticate the downloaded manifest bytes and reject an incomplete census."""
    with tempfile.TemporaryDirectory(prefix="ndc-manifest-") as directory:
        path = str(Path(directory) / "manifest.json")
        async with asyncio.timeout(120):
            receipt = await download_it_and_save(url, path, max_bytes=MAX_MANIFEST_BYTES)
        async with async_open(path, "rb") as manifest_file:
            manifest = json.loads(await manifest_file.read(MAX_MANIFEST_BYTES + 1))
    ndc = manifest["results"]["drug"]["ndc"]
    export_date = ndc.get("export_date")
    if not isinstance(export_date, str) or datetime.date.fromisoformat(export_date).isoformat() != export_date:
        raise ValueError("NDC export_date must be an ISO calendar date")
    partitions = ndc["partitions"]
    total = ndc["total_records"]
    if type(total) is not int or total <= 0 or not isinstance(partitions, list) or not 1 <= len(partitions) <= MAX_PARTITIONS:
        raise ValueError("invalid NDC manifest census")
    for partition in partitions:
        if type(partition.get("records")) is not int or partition["records"] <= 0:
            raise ValueError("invalid NDC partition census")
        if not isinstance(partition.get("file"), str) or not partition["file"]:
            raise ValueError("NDC partition URL is missing")
    if len({partition["file"] for partition in partitions}) != len(partitions):
        raise ValueError("NDC manifest repeats a partition URL")
    if sum(partition["records"] for partition in partitions) != total:
        raise ValueError("NDC manifest and partition census disagree")
    selected = partitions[:1] if sample else partitions
    return {"manifest": receipt, "export_date": export_date,
            "ndc_section_sha256": _canonical_source_sha256(ndc),
            "selected_partitions_sha256": _canonical_source_sha256(selected),
            "complete": not sample, "source_records": total,
            "expected_records": min(max_records, selected[0]["records"]) if sample else total,
            "selected": selected, "sample_limit": max_records if sample else 0}


async def consume_ndc_partitions(manifest: dict, consume_batch) -> dict:
    """Drain bounded concurrent partitions, canceling peer work on any failure."""
    concurrency = int(os.getenv("HLTHPRT_NDC_PARTITION_CONCURRENCY", "4"))
    if not 1 <= concurrency <= 16:
        raise ValueError("NDC partition concurrency must be between 1 and 16")
    semaphore = asyncio.Semaphore(concurrency)

    async def consume_one(partition):
        """Acquire and save one partition while retaining its concurrency slot."""
        async with semaphore:
            timeout = float(os.getenv("HLTHPRT_NDC_PARTITION_TIMEOUT_SECONDS", "14400"))
            if not math.isfinite(timeout) or not 0 < timeout <= 86400:
                raise ValueError("NDC partition deadline must be positive and at most one day")
            async with asyncio.timeout(timeout):
                return await _consume_declared_ndc_partition(partition, manifest["sample_limit"], consume_batch)

    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(consume_one(partition)) for partition in manifest["selected"]]
    receipts = [task.result() for task in tasks]
    if sum(receipt["records"] for receipt in receipts) != manifest["expected_records"]:
        raise ValueError("NDC partition completion census differs from manifest")
    return {"manifest": manifest["manifest"], "export_date": manifest["export_date"],
            "ndc_section_sha256": manifest["ndc_section_sha256"],
            "selected_partitions_sha256": manifest["selected_partitions_sha256"],
            "complete": manifest["complete"],
            "source_records": manifest["source_records"], "partitions": receipts}


async def _consume_declared_ndc_partition(partition: dict, sample_limit: int, consume_batch) -> dict:
    with tempfile.TemporaryDirectory(prefix="ndc-partition-") as directory:
        archive_path = str(Path(directory) / "partition.zip")
        receipt = await download_it_and_save(partition["file"], archive_path, max_bytes=MAX_ARCHIVE_BYTES)
        paths = await unzip(archive_path, directory, max_entries=1,
                            max_entry_size=MAX_ARCHIVE_BYTES, max_total_uncompressed_size=MAX_ARCHIVE_BYTES)
        if len(paths) != 1 or Path(paths[0]).suffix != ".json":
            raise ValueError("NDC archive must contain exactly one JSON document")
        record_count = await _consume_ndc_json(str(paths[0]), sample_limit, consume_batch)
    expected = min(sample_limit, partition["records"]) if sample_limit else partition["records"]
    if record_count != expected:
        raise ValueError("NDC parsed census differs from declared partition")
    return {**receipt, "requested_url": partition["file"], "records": record_count,
            "declared_records": partition["records"]}



async def _consume_ndc_json(path: str, sample_limit: int, consume_batch) -> int:
    batch_size = int(os.getenv("SAVE_PER_PACK", "100"))
    if not 1 <= batch_size <= 500:
        raise ValueError("NDC batch size must be between 1 and 500")
    records = []
    record_count = 0
    async with async_open(path, "rb") as source:
        async for record in ijson.items(source, "results.item", use_float=True):
            records.append(record)
            record_count += 1
            if len(records) == batch_size:
                await consume_batch(records)
                records = []
            if sample_limit and record_count >= sample_limit:
                break
        if records:
            await consume_batch(records)
    return record_count


def _canonical_source_sha256(value) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode()).hexdigest()
