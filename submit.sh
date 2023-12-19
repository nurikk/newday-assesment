#!/usr/bin/env bash
spark-submit --conf spark.pyspark.python=./dist/jobs.pex entrypoint.py --job newday --job-arg output-format=csv --job-arg destination=./results/
spark-submit --conf spark.pyspark.python=./dist/jobs.pex entrypoint.py --job newday --job-arg output-format=parquet --job-arg destination=./results/