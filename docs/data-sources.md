# Data Sources

This page lists the public source websites used by `drug-api`.
The project primarily imports from OpenFDA and exposes data in a normalized local schema.

## Active Import Sources

### OpenFDA
Website: <https://open.fda.gov/>

Used for:

- NDC product and package imports
- drug label imports
- OpenFDA payload fields such as `openfda.rxcui`, `product_ndc`, and `package_ndc`

### FDA Download Catalog
Website: <https://api.fda.gov/download.json>

Used for:

- discovering the current OpenFDA partition files for `drug/ndc` and `drug/label`

## Reference and Enrichment Sources

### RxNorm
Website: <https://www.nlm.nih.gov/research/umls/rxnorm/index.html>

Used for:

- RxNorm-oriented API lookups and terminology alignment
- product/package lookup by RxNorm ID after NDC import populates `product.rxnorm_ids`

### DailyMed
Website: <https://dailymed.nlm.nih.gov/dailymed/>

Used for:

- SPL / set-id oriented label context
- planned or optional media/photo enrichment workflows keyed by `set_id`

## Notes
- `drug-api` does not depend on request-time external calls for normal API responses.
- External websites are treated as ingest-time sources or reference systems, not live runtime dependencies for the main API.
