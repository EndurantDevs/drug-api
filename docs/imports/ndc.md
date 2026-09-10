# NDC Import

## Purpose
Imports OpenFDA NDC data and publishes normalized product and package tables used by search and RxNorm-linked lookups.

## Source Websites
- OpenFDA: <https://open.fda.gov/>
- FDA download catalog: <https://api.fda.gov/download.json>

## Start Command
```bash
python main.py start ndc
```

## Worker
```bash
python main.py worker process.NDC --burst
```

## Main Outputs
- `rx_data.product`
- `rx_data.package`

## Notes
- One coordinator job creates unique, owned product and package staging tables. It awaits every partition and every paired database batch before finalization. Worker startup and shutdown do not publish or drop stages.
- `product.rxnorm_ids` is populated from `openfda.rxcui` during this import.
- Product and package tables are published together.


## Publication evidence

NDC control admission commits its native run before the job becomes visible in
Redis, so an immediate consumer can claim it. Delayed enqueue and queued-cancel
responses cannot overwrite a claimed attempt or its terminal receipt.

The coordinator records the exact downloaded manifest and archive SHA-256 hashes,
byte sizes, the NDC `export_date`, canonical SHA-256 identities of the NDC section
and selected partition list, declared and parsed record counts, and acknowledged product/package
counts. Within one source product, repeated package identities with contradictory
marketing start dates retain an unknown (`NULL`) date instead of choosing an
arbitrary date. The importer logs the affected identity count and still counts
every source package occurrence. Equal dates and dates on different package
identities remain unchanged. Conflicts across source products, or in any other
package field, still fail the attempt. Identical normalized duplicate keys reuse
the stored row; a missing key, conflicting
payload, failed row write, or incomplete partition fails the attempt. Duplicate declared partition URLs are
also rejected. NDC-scoped identities remain stable when another FDA endpoint
changes the shared download catalog. This is
stricter than the previous product-only 95% threshold.

After all partition tasks have finished, finalization locks only the owned stages
while building indexes and auditing their ordered PostgreSQL CSV hashes, schemas,
and row counts. It then takes non-waiting live-table locks, checks the original
product/package OIDs, and swaps both tables. The successful `import_run` update and
`ndc-publication-v1` receipt in its metrics and both table comments commit in that
same transaction. An ownership or terminal-status mismatch rolls back publication.
The receipt describes a native publication, not an independently signed authority
against a privileged database owner.

Test/sample mode records an `ndc_sample` result with `complete=false` and
`published=false`, then drops its owned stages in the same transaction; it never
replaces serving data. After a failure, the coordinator has a separate ten-second
budget to remove only its own unpublished pair, checking the locked OIDs and
ownership comments first. A changed identity or unavailable database leaves the
stages for inspection and logs the cleanup failure. The exact suffix remains in
`metrics.ndc_attempt_id`; the caller's logical `import_id` remains unchanged.
There is no automatic adoption, retry into old stages, or broad cleanup.
A retry needs a new native run; the coordinator does not automatically retry a
failed or canceled run. Legacy queued partition/save
jobs are no longer worker entrypoints; drain the previous worker queue before
switching to this coordinator.

## Execution limits

- `HLTHPRT_NDC_JOB_TIMEOUT_SECONDS` defaults to 86400 (24 hours), replacing the
  previous ARQ default of 300 seconds per partition job. One coordinator runs per
  worker; its awaited partition tasks retain useful parallelism.
- `HLTHPRT_NDC_PARTITION_CONCURRENCY` defaults to 4 and accepts 1 through 16.
- `HLTHPRT_NDC_PARTITION_TIMEOUT_SECONDS` defaults to 14400 (4 hours), with a maximum
  of one day. Failures cancel and drain the other partition tasks.
- `SAVE_PER_PACK` defaults to 100 source records and accepts 1 through 500. Database
  statements use at most 500 normalized rows at a time inside each paired batch.
- The manifest is capped at 16 MiB and 64 partitions; each downloaded archive and
  its extracted JSON are capped at 32 GiB. Database statement/COPY deadlines are
  120 seconds, lock waits at most 5 seconds, and complete finalization at most
  30 minutes. These are failure bounds, not measured throughput guarantees.

If `HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS` is configured for the launcher,
it must allow the intended whole-import job duration. Active cancellation remains
unsupported by the existing control API; process cancellation cannot publish a
partial result. If cancellation overlaps commit, inspect the durable native run
and receipt before attempting a new run. Post-commit event/logging failure cannot
change the successful native state.
