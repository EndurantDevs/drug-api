#!/bin/bash

set -euxo pipefail

uv sync --locked --no-dev

uv run --locked --no-dev --no-sync python main.py start ndc \
&& uv run --locked --no-dev --no-sync python main.py worker process.NDC --burst \
&& uv run --locked --no-dev --no-sync python main.py start label \
&& uv run --locked --no-dev --no-sync python main.py worker process.Labeling --burst
