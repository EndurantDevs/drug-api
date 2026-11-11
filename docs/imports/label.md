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

## Reconcile an interrupted managed publication

The run's `metrics.label_publication_intent` records its stage identity, original OID and source census before partition acquisition. Interrupted stages remain available for review, and failure logs identify the retained stage.

New managed admissions record a publication protocol before enqueue. The dequeuing worker atomically records its runtime claim when it starts the run. Stage creation and intent registration commit together. Before a stage exists, recovery also requires that actual consumer's terminal Kubernetes Job and selected worker image. Older metadata or missing worker evidence causes refusal and preserves the run.

New consumer claims record the immutable image supplied explicitly by their Job. Recovery verifies that retained Job against the image in its claim after a launcher image rollout. Claims written before image binding continue to require the configured image to match their retained Job. Configuration drift leaves those older pre-stage claims unresolved.

For a startup failure before dequeue, the worker-ensure API verifies that the selected admission is queued and has no claim, stage intent or publication receipt. It can then launch a separately named worker derived from the run and terminal Job UID while retaining the original Job. Claimed, terminal or ambiguous run records refuse startup repair. A pending replacement is reused, and concurrent ensures share its deterministic name. The admission lock remains held until an in-flight create and exact-name lookup settle, including cancellation. This can extend the 15-second admission deadline while the bounded requests finish.

Reconcile a selected run after its worker has ended.

```bash
uv run --locked python main.py start label --reconcile-run RUN_ID
```

This command waits for publication locks and reports a verified committed result or marks an exact unpublished attempt failed or canceled. It preserves the stage and serving data. An unpublished attempt loses permission to publish after reconciliation. Missing or ambiguous evidence causes refusal, preserving the unresolved run. A later import uses a distinct run and stage.

Pre-stage recovery must finish while the actual consumer's terminal Job is retained. The launcher preserves terminal Label Jobs, including when starting a distinct run. The configured `HLTHPRT_WORKER_JOB_TTL_SECONDS` still applies and defaults to 86400 seconds after completion. Configure enough retention for the operational recovery window and reconcile before that TTL expires. After Kubernetes deletes the Job, pre-stage recovery refuses and leaves the original run and its idempotency key unresolved. A distinct retry can create a new run while the original remains available for investigation.

## Notes
- The label import is independent from the NDC import.
- `label.set_id` is stored and can be used for DailyMed-oriented workflows.
- Publish uses the same rebuild-and-swap pattern as the NDC import.
