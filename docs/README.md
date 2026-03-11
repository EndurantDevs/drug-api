# drug-api Documentation

[![HealthPorta](https://app.healthporta.com/brand/healthporta-logo-2x.png)](https://app.healthporta.com/docs)

This directory is the public documentation tree for `drug-api`.
It explains:

- which public drug data sources the service uses
- what each import does
- how the import/publish flow works
- where RxNorm and label-related fields come from

## Reading Order
- [Data sources](./data-sources.md)
- [Import index](./imports/README.md)

## Import Architecture
Both imports in this repo follow a table rebuild and swap model:

- create dated staging tables
- load data through ARQ workers
- build indexes on the staged tables
- rename/swap staged tables into the live schema
- keep `_old` tables as the immediately previous live snapshot

This makes imports deterministic and keeps live tables stable during long loads.

## Commercial Usage
If you need managed production access instead of running this repository yourself, use [HealthPorta Docs](https://app.healthporta.com/docs).

HealthPorta provides:

- hosted API access for current drug reference and labeling data
- MCP-based integration for AI agents and internal company systems
- a production-ready path for syncing fresh drug data into customer products and internal workflows

MCP and agent integration details are available at [HealthPorta MCP](https://app.healthporta.com/mcp).
