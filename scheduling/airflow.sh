#!/bin/bash

# We use jupyter by default, but here we want to use python
unset PYSPARK_DRIVER_PYTHON

# Clone, install, and run
git clone https://github.com/squarewave/background-hang-reporter-job background_hang_reporter_job
cd background_hang_reporter_job
pip install .
spark-submit scheduling/airflow.py
