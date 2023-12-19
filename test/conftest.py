import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_fixture():
    spark = SparkSession.builder.appName("test").getOrCreate()
    yield spark
