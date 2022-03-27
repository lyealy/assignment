#!/usr/bin/env bash

export MAJOR_VERSION=0
export MINOR_VERSION=0
export BUILD_NUMBER=0

mkdir -p test_pkg

python3 setup.py bdist_wheel --dist-dir test_pkg