# drug-api
US Drugs API based on OpenFDA data, RxNorm and other sources.

Docker Dev builds can pull:
```shell
docker pull dmytronikolayev/drugs-api
```

Please use .env file for the configuration reference.
You will need to have PostgreSQL and Redis to run this project.
To use API you will need to execute the import for the first time after the container start.

Crontab example to run weekly updates of the data from FDA API:
```shell
15 5 * * 1 docker exec CHANGE_TO_THE_NAME_OF_DRUG_API_CONTAINER /bin/bash -c 'source venv/bin/activate && python main.py start ndc && python main.py worker process.NDC --burst && python main.py start label && python main.py worker process.Labeling --burst' > /dev/null 2>&1
```

## Examples
For testing purposes the API works and getting heavily tested in [Drugs Discount Card](https://pharmacy-near-me.com/drug-discount-card/) Program of [Pharmacy Near Me](https://pharmacy-near-me.com) project. It incudes Medication Search and Prices Comparison with integration with 3rd-party APIs.
