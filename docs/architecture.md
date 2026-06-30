# Drug API Architecture

`drug-api` maintains the drug reference side of HealthPorta: NDC products and
packages, FDA labels, RxNorm links, and label-derived condition evidence.

## Main Boundaries

- `api/` owns HTTP/control-plane endpoints and should stay thin. New request
  parsing and response shaping should use named helpers instead of growing route
  branches.
- `process/` owns importer workflows. Keep download, transform, staging, and
  publish steps separated so failed imports are easy to resume and diagnose.
- `db/` owns connection setup, table metadata, and migration-facing helpers.
- `scripts/ci/` owns repository checks that should be cheap enough for every
  push.
- `tests/` should pin importer contracts, route behavior, and operational guard
  rails with focused fixtures.

## Request And Import Flow

1. Control endpoints schedule or inspect importer work.
2. Import workers download public FDA/OpenFDA payloads.
3. Process modules normalize payloads into staging tables.
4. Successful imports atomically publish live tables.
5. API endpoints serve normalized lookup and control data.

## Where New Code Belongs

- Put importer-specific parsing in the matching `process/` module.
- Put reusable database behavior in `db/`, not in route handlers.
- Put CI/static checks under `scripts/ci/`.
- Prefer small deterministic helpers for Kubernetes/worker manifests and importer
  publish plans.

## Current Hotspots

The first safe refactor target is `api/control_workers.py:_worker_job_manifest`.
It is deterministic, covered by focused tests, and can be decomposed without
touching importer semantics.
