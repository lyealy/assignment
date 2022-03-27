#!/usr/bin/env bash

set -e

mode=$1

export PYTHONPATH=$(pwd)

pytest tests

if [ $mode == 'python' ]; then

  echo "Running mode - python"
  python execution/python_run.py \
         --input_fpath=data/input_data.json \
         --artefacts_path=output_artefacts_python_run

elif [ $mode == 'pyspark' ]; then
  echo "Running mode - pyspark"
  python execution/pyspark_run.py \
        --input_fpath=data/input_data.json \
        --artefacts_path=output_artefacts_pyspark_run
else
  echo "Please provide valid running mode, only accepts python or pyspark"
fi

