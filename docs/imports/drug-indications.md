# Drug Indications Import

`drug-indications` derives drug-to-condition evidence from the local `rx_data.label` table and official clinical terminology tables.
It does not call Ribbon or any other third-party API at request time.

## Command

Run after `ndc` and `label` have published current tables. `label` supplies indication text; `ndc` supplies RxNorm IDs for `/rxnorm/{id}/conditions`.

Run the `healthcare-mrf-api` `clinical-reference` import first on the shared Postgres instance. The importer requires `mrf.code_relationship`, `mrf.code_catalog`, and `mrf.code_synonym` unless test/allow-empty mode is explicitly enabled.

```bash
python main.py start drug-indications --test
python main.py start drug-indications
```

Optional:

```bash
python main.py start drug-indications --import-id 20260525
```

## Tables

The importer stages, indexes, validates, and replaces:

- `rx_data.drug_condition_evidence`

The live API reads these tables directly.

## Clinical Connection

By default the importer looks for clinical terminology in `mrf` on the same shared database. Override with:

- `HLTHPRT_CLINICAL_DB_HOST`
- `HLTHPRT_CLINICAL_DB_PORT`
- `HLTHPRT_CLINICAL_DB_DATABASE`
- `HLTHPRT_CLINICAL_DB_SCHEMA`
- `HLTHPRT_CLINICAL_DB_USER`
- `HLTHPRT_CLINICAL_DB_PASSWORD`

## API Coverage

- `GET /api/v1/drug/ndc/{product_ndc}/conditions`
- `GET /api/v1/drug/rxnorm/{rxnorm_id}/conditions`
- `GET /api/v1/drug/label/{set_id}/condition-evidence`

## Attribution

DailyMed is an NLM resource. Downstream products using this data must preserve:

> This product uses publicly available data from the U.S. National Library of Medicine (NLM), National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the product and does not endorse or recommend this or any other product.
