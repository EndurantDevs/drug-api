# drug-api
US Drugs API based on OpenFDA data, RxNorm and other sources.

Docker Dev builds can be pulled from:
```shell
docker pull dmytronikolayev/drugs-api
```

Please use .env file for the configuration reference.
You will need to have PostgreSQL and Redis to run this project.
To use API you will need to execute the import for the first time after the container start.

Crontab example to run weekly updates of the data from FDA API:
```shell
15 5 * * 1 docker exec CHANGE_TO_THE_NAME_OF_DRUG_API_CONTAINER /usr/local/bin/run_import.sh > /dev/null 2>&1
```

Container-safe one-liner (no container-name hardcoding):
```shell
cid="$(docker ps -qf 'name=docker-drugs_api' | head -n1)"; [ -n "$cid" ] && docker exec "$cid" /usr/local/bin/run_import.sh
```

## RxNorm lookup API
RxNorm lookups are available under:

- `GET /api/v1/drug/rxnorm/{rxnorm_id}/products`
- `GET /api/v1/drug/rxnorm/{rxnorm_id}/packages`

Responses:

- `200` with JSON array when matches are found.
- `404` when no records are found for the provided RxNorm ID.

## RxNorm rollout steps
To roll out RxNorm support safely in an existing environment:

1. Apply Alembic migrations:
   ```shell
   python main.py db migrate
   ```
2. Run a full NDC import cycle to populate `product.rxnorm_ids`:
   ```shell
   python main.py start ndc
   python main.py worker process.NDC --burst
   ```
3. Verify the API with a known RxNorm ID on `/api/v1/drug/rxnorm/{rxnorm_id}/products`.

## ARQ queue isolation
Import workers use dedicated ARQ queues by default to avoid mixing jobs from other services:

- NDC queue: `arq:queue:drug-api-import-ndc`
- Label queue: `arq:queue:drug-api-import-label`

You can override them with environment variables:

- `HLTHPRT_ARQ_QUEUE_NDC` (or `ARQ_QUEUE_NDC`)
- `HLTHPRT_ARQ_QUEUE_LABEL` (or `ARQ_QUEUE_LABEL`)

## Examples
For testing purposes the API works and getting heavily tested in [Drugs Discount Card](https://pharmacy-near-me.com/drug-discount-card/) Program of [Pharmacy Near Me](https://pharmacy-near-me.com) project. It incudes Medication Search and Prices Comparison with integration with 3rd-party APIs.
