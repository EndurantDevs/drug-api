# drug-api

[![HealthPorta](https://app.healthporta.com/brand/healthporta-logo-2x.png)](https://app.healthporta.com/docs)

HealthPorta's drug reference ingestion service for normalized NDC products, packages, labels, and RxNorm-linked lookup support.

## What This Repository Covers
This service maintains the drug-side canonical tables used for:

- NDC product and package search
- FDA label retrieval
- RxNorm-linked product and package lookup
- DailyMed/OpenFDA label-derived condition evidence
- downstream crosswalk support for other HealthPorta services

## Documentation
Public documentation lives in [`docs/README.md`](./docs/README.md).

Key pages:

- [Data sources](./docs/data-sources.md)
- [Import index](./docs/imports/README.md)
- [NDC import](./docs/imports/ndc.md)
- [Label import](./docs/imports/label.md)
- [Drug indications import](./docs/imports/drug-indications.md)
- [Drug indications DevOps](./docs/devops/drug-indications.md)

## Source Families

This repository ingests from public FDA/OpenFDA source systems:

- [OpenFDA](https://open.fda.gov/) (`drug/ndc` and `drug/label` payloads)
- [FDA download catalog](https://api.fda.gov/download.json) (partition discovery)

Reference terminology/context used by the data model:

- [RxNorm](https://www.nlm.nih.gov/research/umls/rxnorm/index.html)
- [DailyMed](https://dailymed.nlm.nih.gov/dailymed/)

See the canonical source registry in [docs/data-sources.md](./docs/data-sources.md).

## Commercial Usage
For production documentation and managed commercial access, see [HealthPorta Docs](https://app.healthporta.com/docs).

HealthPorta can be used as:

- a hosted API layer for current drug reference, NDC, package, and label data
- an MCP-backed data service for AI agents and internal enterprise workflows
- a downstream integration point for customer applications, clinical products, and analytics systems that need fresh drug data

For AI-agent connectivity, see [HealthPorta MCP](https://app.healthporta.com/mcp).

## Local Setup
Use [`.env.example`](./.env.example) as the configuration reference.

Local prerequisites:

- PostgreSQL
- Redis
- Python virtual environment with dependencies installed

Typical startup flow:

```bash
python main.py server start --host 0.0.0.0 --port 8080
```

## Import Quick Start
Run the imports separately.

NDC / product import:

```bash
python main.py start ndc
python main.py worker process.NDC --burst
```

Label import:

```bash
python main.py start label
python main.py worker process.Labeling --burst
```

Drug indication mapping import:

```bash
python main.py start drug-indications --test
python main.py start drug-indications
```

Each import rebuilds staging tables and then swaps them into the live `rx_data` schema.

## Operational Notes
- NDC import publishes `product` and `package` together.
- Label import publishes `label` separately.
- Drug indications import publishes `drug_condition_evidence` from local label data plus official clinical terminology relationships.
- The project uses dedicated ARQ queues for NDC and label imports by default.
- RxNorm lookup support depends on a successful NDC import because `product.rxnorm_ids` is populated from OpenFDA payloads during that import.
- DailyMed/NLM-derived outputs must preserve the required NLM attribution statement from the import docs.
