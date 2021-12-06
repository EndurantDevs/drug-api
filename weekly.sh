#!/bin/bash

set -euxo pipefail

FILE=venv/bin/activate

if test -f "$FILE"; then
	. venv/bin/activate
else
	venvdir=$(mktemp -d)
	python3 -m venv $venvdir
	. $venvdir/bin/activate
	pip install -U pip
	pip install -r requirements.txt
fi

python main.py start ndc
&& python main.py worker process.NDC --burst
&& python main.py start label
&& python main.py worker process.Labeling --burst
