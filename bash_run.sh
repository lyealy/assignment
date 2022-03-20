#!/usr/bin/env bash

set -e

export PYTHONPATH=$(pwd)

pytest tests

python preprocessing/run.py \
  --input_fpath=data/input_data.json \
  --artefacts_path=output_artefacts
