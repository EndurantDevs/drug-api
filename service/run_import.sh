#!/bin/sh
set -e

cd /opt

/opt/venv/bin/python /opt/main.py start ndc
/opt/venv/bin/python /opt/main.py worker process.NDC --burst
/opt/venv/bin/python /opt/main.py start label
/opt/venv/bin/python /opt/main.py worker process.Labeling --burst
