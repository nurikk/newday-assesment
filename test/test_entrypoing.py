import os
import sys
import pyspark
import pytest
from entrypoint import collect_args, create_spark_session, parse_arguments


def test_create_spark_session():
    job_name = "test_job"
    env_vars = {"ENV": "production"}

    spark_session = create_spark_session(job_name, env_vars)
    assert isinstance(spark_session, pyspark.sql.SparkSession)

    for k, v in env_vars.items():
        assert os.environ[k] == v
        assert spark_session.conf.get(f"spark.appMasterEnv.{k}") == v
        assert spark_session.conf.get(f"spark.executorEnv.{k}") == v


def test_collect_args():
    assert collect_args(None) == {}
    assert collect_args(['foo=bar']) == {'foo': 'bar'}
    assert collect_args(['foo=bar', 'baz=qux']) == {'foo': 'bar', 'baz': 'qux'}
    assert collect_args(['foo=bar=baz']) == {'foo': 'bar=baz'}


@pytest.mark.parametrize("args, expected", [
    (None, {}),
    (['foo=bar'], {'foo': 'bar'}),
    (['foo=bar', 'baz=qux'], {'foo': 'bar', 'baz': 'qux'}),
    (['foo=bar=baz'], {'foo': 'bar=baz'}),
])
def test_collect_args_parametrized(args, expected):
    assert collect_args(args) == expected


def test_parse_arguments():
    backup = sys.argv

    sys.argv = ['entrypoint.py', '--job', 'test_job']
    args = parse_arguments()
    assert args.job_name == 'test_job'
    assert args.job_arg == None
    assert args.env_var == None

    sys.argv = ['entrypoint.py', '--job', 'test_job', '--job-arg', 'foo=bar', '--env-var', 'ENV=production']
    args = parse_arguments()
    assert args.job_name == 'test_job'
    assert args.job_arg == ['foo=bar']
    assert args.env_var == ['ENV=production']

    sys.argv = backup


def test_parse_arguments_failure():
    backup = sys.argv

    sys.argv = ['entrypoint.py']
    with pytest.raises(SystemExit):
        parse_arguments()

    sys.argv = backup
