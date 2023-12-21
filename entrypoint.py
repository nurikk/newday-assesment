import pyspark
import argparse
import logging
import os
import importlib
import time
import traceback
import sys
import findspark

findspark.init()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name',
                        help="The name of the job module you want to run.")
    parser.add_argument('--job-arg', type=str, action='append',
                        help="Extra arguments to send to the PySpark job, the format is space delimited string. example: --job-arg foo=bar --job-arg date=2012-12-12\ 12:12:12")
    parser.add_argument('--env-var', type=str, action='append',
                        help="ENV vars to be passed to cluster. example: --env-var foo=bar --env-var ENV=production")
    return parser.parse_args()


def create_spark_session(job_name: str, env_vars: dict) -> pyspark.sql.SparkSession:
    temp_spark = pyspark.sql.SparkSession.builder.appName(job_name)
    for k, v in env_vars.items():
        os.environ[k] = v
        temp_spark = temp_spark.config(f"spark.appMasterEnv.{k}", v) \
            .config(f"spark.executorEnv.{k}", v)

    return temp_spark.getOrCreate()


def collect_args(args: list) -> dict:
    dict_args = {}
    args = args or []
    for arg_str in args:
        key, value = arg_str.split('=', 1)
        dict_args[key] = value
    return dict_args


def main(args: argparse.Namespace) -> None:
    job_args = collect_args(args.job_arg)
    env_vars = collect_args(args.env_var)
    logger.info('\nRunning job %s...\njob-args is %s\nenv-vars is %s\n' %
                (args.job_name, job_args, env_vars))

    spark = create_spark_session(args.job_name, env_vars)
    job_module = importlib.import_module('jobs.%s' % args.job_name)
    start = time.time()
    try:
        job_module.perform(spark, job_args)
    except Exception as e:
        logger.error(f'Error: {str(e)}\nTrace: {traceback.format_exc()}')
        sys.exit(1)

    end = time.time()
    logger.info("\nExecution of job %s took %s seconds" %
                (args.job_name, end - start))


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
