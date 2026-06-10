# AGENTS.md

## Project Snapshot
- Service: `drug-api` (Python/Sanic + SQLAlchemy async/PostgreSQL via asyncpg + ARQ/Redis).
- Primary datasets: OpenFDA `drug/ndc` and `drug/label` partition files.
- Runtime entrypoint in container: `/usr/local/bin/start_api.sh`.

## Import Architecture (Source of Truth)
- Imports are table-rebuild + swap, not row-by-row in-place updates.
- NDC import creates `rx_data.product_<YYYYMMDD>` and `rx_data.package_<YYYYMMDD>`, builds indexes, then renames/switches to `rx_data.product` and `rx_data.package`.
- Label import creates `rx_data.label_<YYYYMMDD>`, builds indexes, then renames/switches to `rx_data.label`.
- Because tables are recreated from current models during import, schema changes present in models are expected to appear after a successful full import cycle.

## RxNorm Notes
- `Product.rxnorm_ids` exists in model (`db/models.py`) and is populated during NDC import from `openfda.rxcui`.
- RxNorm API endpoints:
  - `GET /api/v1/drug/rxnorm/{rxnorm_id}/products`
  - `GET /api/v1/drug/rxnorm/{rxnorm_id}/packages`
- If RxNorm lookups are empty after rollout, verify a full NDC import cycle completed (`start ndc` + `worker process.NDC --burst`).

## SPL / Label Notes
- `Label.set_id` is imported from OpenFDA label data and stored in `rx_data.label.set_id`.
- Product rows expose `spl_id`; label lookup logic first checks `Label.id == spl_id`, then fallback `Label.set_id == spl_id`.
- `set_id` is returned in API responses only when a nested `label` object is returned (label endpoints), not on plain product/package endpoints.

## DailyMed Package Photo Data
- OpenFDA label payloads do not provide package photo URLs.
- DailyMed provides media URLs by SPL set id:
  - `/dailymed/services/v2/spls/{SETID}/media.json`
- NDC to media lookup path:
  1. NDC -> SETID: `/dailymed/services/v2/spls.json?ndc={NDC}`
  2. SETID -> media URLs: `/dailymed/services/v2/spls/{SETID}/media.json`

## Recommended Implementation Strategy (No Per-Request External Calls)
- Keep request handlers read-only against local DB.
- Add an offline enrichment phase after label import:
  1. Read distinct `rx_data.label.set_id`.
  2. Fetch DailyMed media for each `set_id`.
  3. Store results in a local table (for example `rx_data.label_media` with `set_id`, `name`, `mime_type`, `url`, `synced_at`).
  4. Join/enrich label responses from local DB only.
- Continue treating external APIs as ingest-time dependencies, not request-time dependencies.
