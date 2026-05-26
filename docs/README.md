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
- [Drug indications DevOps](./devops/drug-indications.md)

## Import Architecture
Imports in this repo use importer-specific publish models:

- NDC and label imports create dated staging tables, load through ARQ workers, build indexes, then swap staged tables into the live schema with `_old` rollback backups.
- Drug indications creates dated staging tables, builds indexes, validates row counts, then directly replaces the live indication tables without `_old` rollback backups.

Both patterns keep live tables stable during long loads. Check the per-import runbook before cleanup or rollback work.

## Commercial Usage
If you need managed production access instead of running this repository yourself, use [HealthPorta Docs](https://app.healthporta.com/docs).

HealthPorta provides:

- hosted API access for current drug reference and labeling data
- MCP-based integration for AI agents and internal company systems
- a production-ready path for syncing fresh drug data into customer products and internal workflows

MCP and agent integration details are available at [HealthPorta MCP](https://app.healthporta.com/mcp).
