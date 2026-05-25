# Data Sources

This page is the canonical source registry for `drug-api`.

## Active Import Source Registry

| Source website | Dataset families used | Importers using it | Main outputs |
| --- | --- | --- | --- |
| <https://open.fda.gov/> | OpenFDA `drug/ndc` and `drug/label` public payloads | `ndc`, `label`, `drug-indications` | normalized `rx_data.product`, `rx_data.package`, `rx_data.label`, derived condition evidence |
| Shared `healthcare-mrf-api` clinical terminology tables | RxNorm-to-condition/treatment relationships from official terminology imports | `drug-indications` | supplemental `clinical_rxnorm_relationship` evidence in `rx_data.drug_condition_evidence` and `rx_data.drug_treatment_mapping` |
| <https://api.fda.gov/download.json> | OpenFDA partition discovery feed | `ndc`, `label` | current partition URL resolution before import |

## Reference/Terminology Sources

| Source website | How it is used |
| --- | --- |
| <https://www.nlm.nih.gov/research/umls/rxnorm/index.html> | Terminology alignment for RxNorm-linked API lookups. RxNorm IDs are populated from OpenFDA `openfda.rxcui` during NDC import. |
| <https://dailymed.nlm.nih.gov/dailymed/> | SPL/set-id reference context and indication text behind `drug-indications` condition evidence. |

## Notes

- `drug-api` serves local PostgreSQL tables at request time.
- External websites are import-time or reference dependencies, not required per-request upstream calls.
- Import freshness depends on each importer’s latest successful publish/swap cycle.
- Products using NLM-derived data must preserve the attribution statement documented in [imports/drug-indications.md](./imports/drug-indications.md).
