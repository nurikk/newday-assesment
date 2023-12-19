#!/usr/bin/env bash
spark-submit --conf spark.pyspark.python=./dist/jobs.pex entrypoint.py --job newday --job-arg output_format=csv --job-arg destination=./results/
spark-submit --conf spark.pyspark.python=./dist/jobs.pex entrypoint.py --job newday --job-arg output_format=parquet --job-arg destination=./results/