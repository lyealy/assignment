#!/usr/bin/env bash

set -e

mode=$1

# PYTHONPATH is not required if you have already installed the preprocessing package
get_package=$(pip list | grep preprocessing -Eo || true)

if [ -z "$get_package" ]; then
  echo "Package is not installed, use source code"
  export PYTHONPATH=$(pwd)
else
  echo "Package is already installed"
fi

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

