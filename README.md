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
- [Architecture](./docs/architecture.md)
- [Readability budget](./docs/readability.md)
- [Test coverage ratchet](./docs/test-coverage.md)
- [Commit message style](./docs/commit-messages.md)

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
Use Python 3.13, PostgreSQL 18 and Redis 7. Start PostgreSQL and Redis locally,
then run from the repository root:

```bash
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
cp .env.example .env
```

Create a dedicated application role using your local PostgreSQL administrator
(shown as `postgres`), then put that role's password in `.env`:

```bash
createuser --host=127.0.0.1 --username=postgres --no-superuser \
  --no-createdb --no-createrole --pwprompt drug_api
createdb --host=127.0.0.1 --username=postgres --owner=drug_api drug_api
psql --host=127.0.0.1 --username=drug_api --dbname=drug_api \
  --command='CREATE SCHEMA IF NOT EXISTS rx_data'
python main.py db migrate
python main.py server start --host 127.0.0.1 --port 8080
```

In another terminal, `curl http://127.0.0.1:8080/api/v1/healthcheck/live`
returns `{"status":"OK","release":"local"}`. Catalog endpoints need a
successful import. Configuration is loaded from `.env` by `main.py`.

### Synthetic import checks

These existing tests feed synthetic FDA records through the NDC parser, check
RxNorm mapping and verify table publication behavior. They need no database,
Redis server, source downloads or managed service:

```bash
python -m pytest -q tests/process/test_ndc_rxnorm_mapping.py \
  tests/process/test_import_table_switching.py tests/api/test_healthcheck.py
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for focused development checks.

### Runtime image

The runtime image installs `requirements.txt`; contributor tools stay in
`requirements-dev.txt`. Build for your native architecture:

```bash
docker build --build-arg HLTHPRT_SOURCE_COMMIT="$(git rev-parse HEAD)" --tag drug-api:local .
docker run --rm --env-file .env drug-api:local /opt/venv/bin/python /opt/main.py --help
```

For a running container, configure PostgreSQL and Redis addresses reachable
from that container, then publish port 8080. On Docker Desktop, services on
the host can use `host.docker.internal` in place of `127.0.0.1`.

## Import Quick Start
Run the imports separately against the local database. These commands download
the full FDA source partitions; use the synthetic checks above for a small,
offline example.

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

Drug indication mapping additionally requires the public `healthcare-mrf-api`
clinical-reference tables. Follow [its prerequisites](./docs/imports/drug-indications.md)
and configure `HLTHPRT_CLINICAL_DB_PORT=5432` for the local PostgreSQL setup above
(the importer's default clinical port is 5440):

```bash
python main.py start drug-indications --test
python main.py start drug-indications
```

Each import rebuilds staging tables and then swaps them into the live `rx_data` schema.

## Optional Control Integration

The CLI imports and catalog API run with your PostgreSQL and Redis services.
The authenticated `/control/v1` API lets a controller enqueue and inspect imports
and manage workers. Configure `HLTHPRT_CONTROL_API_TOKEN` to use it; requests are
denied when no token is configured. Worker launch defaults to a local process;
Kubernetes launch requires explicit `HLTHPRT_WORKER_LAUNCHER=kubernetes` and worker
Job configuration.

Import progress callbacks are disabled when `HLTHPRT_IMPORT_CONTROL_URL` and
`HP_IMPORT_CONTROL_BASE_URL` are unset. An explicitly configured URL receives
best-effort run status events. No managed HealthPorta service is required.

## Operational Notes
- NDC import publishes `product` and `package` together.
- Label import publishes `label` separately.
- Drug indications import publishes `drug_condition_evidence` from local label data plus official clinical terminology relationships.
- The project uses dedicated ARQ queues for NDC and label imports by default.
- RxNorm lookup support depends on a successful NDC import because `product.rxnorm_ids` is populated from OpenFDA payloads during that import.
- DailyMed/NLM-derived outputs must preserve the required NLM attribution statement from the import docs.
