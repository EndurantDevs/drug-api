#!/bin/sh
set -e

cd /opt

nginx
exec /opt/venv/bin/python /opt/main.py server start --host 127.0.0.1 --port 8081
