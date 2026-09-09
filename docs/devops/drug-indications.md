# Drug Indications DevOps

## Prerequisite

Run NDC and label imports first. NDC supplies RxNorm IDs; label supplies indication text:

```bash
uv run --locked python main.py start ndc
uv run --locked python main.py worker process.NDC --burst
uv run --locked python main.py start label
uv run --locked python main.py worker process.Labeling --burst
```

For full condition/treatment coverage, also run `healthcare-mrf-api` clinical terminology first:

```bash
cd ../healthcare-mrf-api
uv run --locked python main.py start clinical-reference --import-id 20260525
cd ../drug-api
```

## Smoke Run

```bash
uv run --locked python main.py start drug-indications --test --import-id smoke
```

## Full Run

```bash
uv run --locked python main.py start drug-indications --import-id 20260525
```

The importer creates a stage evidence table, extracts indication evidence from local labels plus official `mrf.code_relationship`/`mrf.code_synonym` rows, builds indexes at the end, validates row counts, and replaces the live evidence table only after validation. It does not keep `_old` rollback tables.

## Attribution

Any product using DailyMed/NLM-derived condition evidence must include:

> This product uses publicly available data from the U.S. National Library of Medicine (NLM), National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the product and does not endorse or recommend this or any other product.
