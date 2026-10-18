# NDC Import

## Purpose
Imports OpenFDA NDC data and publishes normalized product and package tables used by search and RxNorm-linked lookups.

## Source Websites
- OpenFDA: <https://open.fda.gov/>
- FDA download catalog: <https://api.fda.gov/download.json>

## Start Command
```bash
uv run --locked python main.py start ndc
```

## Worker
```bash
uv run --locked python main.py worker process.NDC --burst
```

## Main Outputs
- `rx_data.product`
- `rx_data.package`

## Notes
- One coordinator job creates unique, owned acquisition and candidate heaps from the product and package models. It awaits every partition and every paired COPY batch before finalization. Worker startup and shutdown do not publish or drop stages.
- `product.rxnorm_ids` is populated from `openfda.rxcui` during this import.
- Product and package tables are published together.

## Publication authority

`HLTHPRT_NDC_PUBLICATION_MODE` is trusted worker-process configuration, never a
request parameter. It accepts exactly `direct` or `handoff`; other values fail
before the attempt creates stages. The mode is captured once per attempt, with
no metadata detection or fallback to a different publisher.

- `direct` is the default for legacy compatibility. The coordinator retains its
  paired-table swap and existing `ndc-publication-v1` result. It is not a completed
  protected-publication deployment.
- `handoff` requires `DB_SCHEMA=rx_data` (also the default schema) and a nonempty,
  unpadded `HLTHPRT_IMPORT_NODE_ID`. Managed runs must already have that same node
  identity; standalone runs record it when created. Run IDs must match
  `[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}` before any database or acquisition work.
  A completed coordinator returns `ndc-stage-handoff-v1` and remains
  `finalizing`; it never claims that the candidate has become current.

A deployment using protected publication must explicitly select `handoff` for
this worker and provision the matching publisher before treating NDC as complete
or retiring legacy write guards. The CLI and control API use the same process
selection; payloads cannot choose a node or publication authority.


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
package field, still fail the attempt. Every normalized source occurrence is
copied to isolated acquisition heaps. Missing or whitespace-only keys and
conflicting duplicate payloads, within or across batches, are checked set-wise
before either candidate projection is copied. Identical duplicates share one
completed row. A failed COPY or incomplete partition fails the attempt. Duplicate declared partition URLs are
also rejected. NDC-scoped identities remain stable when another FDA endpoint
changes the shared download catalog. This is
stricter than the previous product-only 95% threshold.

After all partition tasks have finished, finalization locks only the owned heaps,
validates inputs, copies both model projections, then creates their keys and
indexes. An indexed set-based check rejects missing package parents without
per-row foreign keys. Ordered PostgreSQL CSV audits bind candidate hashes,
schemas and row counts. Direct mode then takes non-waiting live-table locks, checks the original
product/package OIDs, and swaps both tables. The successful `import_run` update and
`ndc-publication-v1` receipt in its metrics and both table comments commit in that
same transaction. An ownership or terminal-status mismatch rolls back publication.
The receipt describes a native publication, not an independently signed authority
against a privileged database owner.

Handoff mode instead checks the original serving OIDs and atomically records
`ndc-stage-handoff-v1` under `metrics.ndc_handoff`, with matching compact comments
on both candidate tables. The receipt binds the run, attempt, schema, database
and ledger OIDs, predecessor OIDs, source acquisition and audited candidate pair.
Its digest uses sorted compact UTF-8 JSON without ASCII escaping, excluding only
`handed_off_at` and `handoff_sha256`. The run/node/attempt compare-and-set requires
an active, nonsample run without a prior handoff or terminal receipt. It records
`finalizing`, phase `ndc stages awaiting publication`, and no finished timestamp
or error. Only the acquisition heaps are dropped in this transaction; current
serving tables are unchanged. The ledger and comments commit together or roll
back together. Completion failures restore the in-memory candidate counts.

Handoff holds a non-waiting publication fence and reader-compatible locks on
existing serving tables through commit. Direct publishers, including the legacy
compatibility call, use the same fence after index preparation. Absent table
names rely on that cooperative fence; downstream admission and generation checks
remain required for later changes or noncooperating publishers.

After handoff commits, the publisher owns candidate retention, validation,
publication and final success/failure. Producer failure or cancellation cleanup
cannot delete handed-off stages or relabel the run. A lost commit acknowledgement
does not authorize another attempt over the same stages: inspect the durable
ledger and let the publisher reconcile it. Handoff is not itself publication or
independent authorization of candidate contents.

### Read-only dependency capture

`drug_snapshot_runtime.ndc_publication.capture_ndc_dependency` requires an existing
read-only, repeatable-read transaction. It supports the native heap pair, the
protected native view pair, and received snapshots carrying the existing paired
`ndc.csv.v1` adopted receipt. For received views, each receipt table identifies the
receiver's backing heap with `relation_oid`, `row_count`, `size_bytes`, `sha256`
and `columns`. `columns` is the ordered list of `[name, type, not_null]` arrays,
not the native receipt's column-object form.
`source_publication.native_publication` remains the unchanged original native
receipt. Both views must carry the same complete adopted receipt. The
installer must commit those comments with its protected alias switch; canonical
view OIDs are not backing heap identities.

Received views use the same fixed product/package projections, same-schema
protected ownership and dependency checks as native views. The reader also checks
the complete locally declared primary-key/GIN indexes and trusted operator-class
code before scanning either view. A received snapshot does not create or require
a local execution of the original producer. The four-field
`ndc-native-dependency.v1` result still hashes the original native receipt and
product/package payloads, not receiver-local OIDs.

Capture performs full counts and ordered native COPY audits of both tables on
every invocation, including native and legacy routes. It is a producer dependency
capture API, not a constant-time API lookup or a persistent validation cache.

Test/sample mode records an `ndc_sample` result with `complete=false` and
`published=false`, then drops its owned stages in the same transaction; it never
replaces serving data. After a failure, the coordinator has a separate ten-second
budget to remove only its own unpublished heaps, checking the run, locked OIDs and
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
- `SAVE_PER_PACK` defaults to 100 source records and accepts 1 through 500. Native
  COPY uses at most 5000 normalized rows at a time inside each paired batch.
- The manifest is capped at 16 MiB and 64 partitions; each downloaded archive and
  its extracted JSON are capped at 32 GiB. Model projections are capped at 32 GiB
  each, with 300-second COPY deadlines. Statement and audit deadlines are
  120 seconds, lock waits at most 5 seconds, and complete finalization at most
  30 minutes. These are failure bounds, not measured throughput guarantees.

If `HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS` is configured for the launcher,
it must allow the intended whole-import job duration. Active cancellation remains
unsupported by the existing control API; process cancellation cannot publish a
partial result. If cancellation overlaps commit, inspect the durable native run
and receipt before attempting a new run. Post-commit event/logging failure cannot
change the committed native state.
