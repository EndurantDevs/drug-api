# Drug Indications DevOps

## Prerequisite

Run NDC and label imports first. NDC supplies RxNorm IDs; label supplies indication text:

```bash
python main.py start ndc
python main.py worker process.NDC --burst
python main.py start label
python main.py worker process.Labeling --burst
```

For full condition/treatment coverage, also run `healthcare-mrf-api` clinical terminology first:

```bash
cd ../healthcare-mrf-api
python main.py start clinical-reference --import-id 20260525
cd ../drug-api
```

## Smoke Run

```bash
python main.py start drug-indications --test --import-id smoke
```

## Full Run

```bash
python main.py start drug-indications --import-id 20260525
```

The importer creates stage tables, extracts indication evidence from local labels and available `mrf.code_relationship` rows, builds indexes at the end, validates row counts, and replaces live tables only after validation. It does not keep `_old` rollback tables.

## Attribution

Any product using DailyMed/NLM-derived condition evidence must include:

> This product uses publicly available data from the U.S. National Library of Medicine (NLM), National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the product and does not endorse or recommend this or any other product.
