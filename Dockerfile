FROM public.ecr.aws/docker/library/python:3.14-slim-trixie

WORKDIR /wheels
ADD ./requirements-dev.txt /wheels
ADD ./requirements.txt /wheels

WORKDIR /opt
RUN apt-get update \
    && if apt-cache show libaio1t64 >/dev/null 2>&1; then LIBAIO_PKG=libaio1t64; else LIBAIO_PKG=libaio1; fi \
    && apt-get install -y --no-install-recommends "${LIBAIO_PKG}" gcc g++ make libc6-dev pkg-config git curl nginx \
    && python3 -m venv venv \
    && . venv/bin/activate \
    && pip install --no-compile --upgrade pip \
    && pip install --no-compile -r /wheels/requirements-dev.txt -f /wheels \
    && rm -rf /wheels \
    && rm -rf /root/.cache/pip/* \
    && find . -name '*.pyc' -delete \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn /opt/venv /opt/venv314

ARG HLTHPRT_LOG_CFG=./logging.yaml
ARG HLTHPRT_RELEASE="dev"
ARG HLTHPRT_ENVIRONMENT=test

ARG HLTHPRT_DB_POOL_MAX_SIZE=10
ARG HLTHPRT_DB_POOL_MIN_SIZE=1

ARG HLTHPRT_DB_HOST=localhost
ARG HLTHPRT_DB_PORT=5432
ARG HLTHPRT_DB_DATABASE=healthporta
ARG HLTHPRT_DB_SCHEMA='rx_data'
ARG HLTHPRT_DB_USER=dmytro
ARG HLTHPRT_DB_PASSWORD=''
ARG HLTHPRT_REDIS_ADDRESS=redis://localhost:6379

ARG HLTHPRT_MAIN_RX_JSON_URL='https://api.fda.gov/download.json'
ARG HLTHPRT_SAVE_PER_PACK=100


ENV HLTHPRT_LOG_CFG=${HLTHPRT_LOG_CFG}
ENV HLTHPRT_RELEASE=${HLTHPRT_RELEASE}
ENV HLTHPRT_ENVIRONMENT=${HLTHPRT_ENVIRONMENT}
ENV HLTHPRT_DB_POOL_MIN_SIZE=${HLTHPRT_DB_POOL_MIN_SIZE}
ENV HLTHPRT_DB_POOL_MAX_SIZE=${HLTHPRT_DB_POOL_MAX_SIZE}

ENV HLTHPRT_DB_HOST=${HLTHPRT_DB_HOST}
ENV HLTHPRT_DB_PORT=${HLTHPRT_DB_PORT}
ENV HLTHPRT_DB_USER=${HLTHPRT_DB_USER}
ENV HLTHPRT_DB_PASSWORD=${HLTHPRT_DB_PASSWORD}
ENV HLTHPRT_DB_DATABASE=${HLTHPRT_DB_DATABASE}
ENV HLTHPRT_DB_SCHEMA=${HLTHPRT_DB_SCHEMA}

ENV HLTHPRT_REDIS_ADDRESS=${HLTHPRT_REDIS_ADDRESS}
ENV HLTHPRT_MAIN_RX_JSON_URL=${HLTHPRT_MAIN_RX_JSON_URL}
ENV HLTHPRT_SAVE_PER_PACK=${HLTHPRT_SAVE_PER_PACK}

ADD service/nginx.conf /etc/nginx/nginx.conf
ADD service/start_api.sh /usr/local/bin/start_api.sh
ADD service/run_import.sh /usr/local/bin/run_import.sh
RUN chmod a+x /usr/local/bin/start_api.sh /usr/local/bin/run_import.sh

COPY api/ /opt/api/
COPY db/ /opt/db/
COPY alembic/ /opt/alembic/
COPY process/ /opt/process/
COPY logging.yaml main.py alembic.ini /opt/

EXPOSE 8080
CMD ["/usr/local/bin/start_api.sh"]
