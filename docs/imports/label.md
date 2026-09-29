# Label Import

## Purpose
Imports OpenFDA drug label records and publishes the normalized `label` table used by label and SPL-oriented API responses.

## Source Websites
- OpenFDA: <https://open.fda.gov/>
- FDA download catalog: <https://api.fda.gov/download.json>

## Start Command
```bash
uv run --locked python main.py start label
```

## Worker
```bash
uv run --locked python main.py worker process.Labeling --burst
```

## Main Outputs
- `rx_data.label`

## Notes
- The label import is independent from the NDC import.
- `label.set_id` is stored and can be used for DailyMed-oriented workflows.
- Ordinary publication rebuilds and swaps the serving table. Once the serving table is protected,
  managed imports produce an attempt-owned stage and remain `finalizing` until its publisher acknowledges it.
- Restart Label workers after publication protection is removed. A worker that entered
  protected mode refuses ordinary imports rather than enqueueing batches into an uncreated staging table.

## Failed protected stages

The producer removes a failed stage only after locking and matching its run, attempt,
recorded OID, ownership comment, and current database role. Completed handoffs and
publication admission markers prevent deletion. Cleanup failure preserves the original
import error and retains the stage for inspection.

A terminated process or unavailable database can leave a stage behind. A hard worker
exit after claim can also leave the run `running` and block later protected admissions;
this version has no owner-liveness-verified automatic recovery. Do not retry or mark
that run failed until its ownership has been independently resolved. After the
responsible run controller has established that the exact run is `failed`, an
operator can retry the same bounded cleanup helper.
Do not change an active or finalizing run to failed merely to permit cleanup, remove
admission markers, or infer ownership from a table name.

With the ordinary producer's database settings, replace the synthetic run ID below
with the exact failed run. The helper verifies the durable attempt and OID from the
run record again under locks and refuses changed, admitted, or protected relations.

```python
import asyncio
import json
from db.connection import Database
from process.label_stage import has_discarded_failed_label_stage

async def cleanup():
    database = Database()
    run_id = "synthetic-failed-run"
    schema = "rx_data"
    try:
        await database.connect()
        metrics = await database.scalar(
            f"SELECT metrics FROM {schema}.import_run WHERE run_id=:run_id AND importer='label' AND status='failed'",
            run_id=run_id,
        )
        if not metrics:
            raise RuntimeError("exact failed run is unavailable")
        attempt = metrics["label_attempt_id"]
        comment = json.dumps({"format": "label-stage-v1", "run_id": run_id, "attempt_id": attempt}, sort_keys=True)
        async with asyncio.timeout(10):
            return await has_discarded_failed_label_stage(database, schema, run_id, attempt, metrics["label_stage_oid"], comment)
    finally:
        await database.disconnect()

print(asyncio.run(cleanup()))
```

A false result means no deletion occurred. An ownership, lock, or database error also
requires inspection; preserve the retained evidence rather than forcing deletion.
