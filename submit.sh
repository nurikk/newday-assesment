#!/usr/bin/env bash
spark-submit --conf spark.pyspark.python=./dist/jobs.pex \
								 entrypoint.py --job pi --job-arg date=2021-12-12 \
								 --env-var partitions=4 --env-var sample_size=20000000