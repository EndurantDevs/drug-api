# drug-api

[![HealthPorta](https://app.healthporta.com/brand/healthporta-logo-2x.png)](https://app.healthporta.com/docs)

HealthPorta's drug reference ingestion service for normalized NDC products, packages, labels, and RxNorm-linked lookup support.

## What This Repository Covers
This service maintains the drug-side canonical tables used for:

- NDC product and package search
- FDA label retrieval
- RxNorm-linked product and package lookup
- downstream crosswalk support for other HealthPorta services

## Documentation
Public documentation lives in [`docs/README.md`](./docs/README.md).

Key pages:

- [Data sources](./docs/data-sources.md)
- [Import index](./docs/imports/README.md)
- [NDC import](./docs/imports/ndc.md)
- [Label import](./docs/imports/label.md)

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

Each import rebuilds staging tables and then swaps them into the live `rx_data` schema.

## Operational Notes
- NDC import publishes `product` and `package` together.
- Label import publishes `label` separately.
- The project uses dedicated ARQ queues for NDC and label imports by default.
- RxNorm lookup support depends on a successful NDC import because `product.rxnorm_ids` is populated from OpenFDA payloads during that import.
