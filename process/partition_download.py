"""Shared downloader for FDA JSON partition archives."""

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Any

import ijson
from aiofile import async_open
from async_unzip import unzip

from process.ext.utils import download_it_and_save
from process.live_progress import enqueue_live_progress


@dataclass(frozen=True)
class PartitionDownloadSpec:
    importer: str
    model: str
    result_job_name: str
    start_message: str
    item_label: str


@dataclass(frozen=True)
class _PartitionContext:
    run_id: str | None
    max_records: int
    partition_records: int
    partition_index: int
    partition_count: int


async def download_partition_content(ctx: dict[str, Any], task: dict[str, Any], spec: PartitionDownloadSpec) -> int:
    """Download a partition archive, stream its JSON rows, and enqueue parse batches."""
    partition_context = _partition_context(ctx, task)
    print(spec.start_message, task.get('file'))
    _enqueue_partition_started(partition_context, spec)

    redis = ctx['redis']
    send_count = await _download_and_enqueue_batches(redis, task, partition_context, spec)
    print('Added taks: ', send_count)
    return 1


def _partition_context(ctx: dict[str, Any], task: dict[str, Any]) -> _PartitionContext:
    max_records = int(task.get('max_records') or 0)
    return _PartitionContext(
        run_id=task.get('run_id') or ctx.get('control_run_id') or ctx.get('context', {}).get('control_run_id'),
        max_records=max_records,
        partition_records=int(task.get('partition_records') or max_records or 0),
        partition_index=int(task.get('partition_index') or 1),
        partition_count=int(task.get('partition_count') or 1),
    )


def _enqueue_partition_started(partition_context: _PartitionContext, spec: PartitionDownloadSpec) -> None:
    enqueue_live_progress(
        run_id=partition_context.run_id,
        importer=spec.importer,
        status="running",
        phase=f"{spec.importer} downloading partition",
        unit="partitions",
        done=max(partition_context.partition_index - 1, 0),
        total=partition_context.partition_count,
        message=f"downloading partition {partition_context.partition_index}/{partition_context.partition_count}",
    )


async def _download_and_enqueue_batches(
    redis: Any,
    task: dict[str, Any],
    partition_context: _PartitionContext,
    spec: PartitionDownloadSpec,
) -> int:
    with tempfile.TemporaryDirectory() as tmpdirname:
        archive_path = Path(task.get('file'))
        tmp_filename = str(PurePath(str(tmpdirname), archive_path.name))
        json_tmp_file = str(PurePath(str(tmpdirname), archive_path.stem))

        await download_it_and_save(task.get('file'), tmp_filename)
        await unzip(tmp_filename, tmpdirname)

        return await _enqueue_batches_from_json(redis, task, partition_context, spec, json_tmp_file)


async def _enqueue_batches_from_json(
    redis: Any,
    task: dict[str, Any],
    partition_context: _PartitionContext,
    spec: PartitionDownloadSpec,
    json_tmp_file: str,
) -> int:
    async with async_open(json_tmp_file, 'r') as afp:
        batch_counter = 0
        read_counter = 0
        send_counter = 0
        batch_task_dict = _new_batch_task(task, partition_context, spec)

        async for source_record in ijson.items(afp, 'results.item'):
            batch_task_dict['results'].append(source_record)
            read_counter += 1
            if partition_context.max_records and read_counter >= partition_context.max_records:
                break
            if batch_counter == int(os.environ.get('SAVE_PER_PACK', 100)):
                await _enqueue_batch(redis, batch_task_dict, read_counter, partition_context, spec)
                batch_task_dict = _new_batch_task(task, partition_context, spec)
                batch_counter = -1
                send_counter += 1
            batch_counter += 1
        batch_task_dict['batch_end'] = read_counter
        await redis.enqueue_job(spec.result_job_name, batch_task_dict)
        _enqueue_partition_parsed(read_counter, partition_context, spec)
        return send_counter + 1


def _new_batch_task(
    task: dict[str, Any],
    partition_context: _PartitionContext,
    spec: PartitionDownloadSpec,
) -> dict[str, Any]:
    return {
        'what': task.get('what'),
        'model': spec.model,
        'results': [],
        'run_id': partition_context.run_id,
        'partition_records': partition_context.partition_records,
    }


async def _enqueue_batch(
    redis: Any,
    batch_task_dict: dict[str, Any],
    read_counter: int,
    partition_context: _PartitionContext,
    spec: PartitionDownloadSpec,
) -> None:
    batch_task_dict['batch_end'] = read_counter
    await redis.enqueue_job(spec.result_job_name, batch_task_dict)
    enqueue_live_progress(
        run_id=partition_context.run_id,
        importer=spec.importer,
        status="running",
        phase=f"{spec.importer} parsing records",
        unit="records",
        done=read_counter,
        total=partition_context.partition_records or None,
        message=f"parsed {read_counter} {spec.item_label}",
    )


def _enqueue_partition_parsed(
    read_counter: int,
    partition_context: _PartitionContext,
    spec: PartitionDownloadSpec,
) -> None:
    enqueue_live_progress(
        run_id=partition_context.run_id,
        importer=spec.importer,
        status="running",
        phase=f"{spec.importer} partition parsed",
        unit="records",
        done=read_counter,
        total=partition_context.partition_records or None,
        message=f"parsed {read_counter} {spec.item_label}",
    )
