import pytest
from pyspark.sql.types import StructType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

from jobs.newday.schema import datasets


@pytest.fixture
def ratings_fixture(spark_fixture):
    yield spark_fixture.createDataFrame(
        schema=datasets['http://files.grouplens.org/datasets/movielens/ml-1m.zip']['ml-1m/ratings.dat'],
        data=[
            {"UserID": 1, "MovieID": 1, "Rating": 1},
            {"UserID": 1, "MovieID": 2, "Rating": 2},
            {"UserID": 1, "MovieID": 3, "Rating": 3},
            {"UserID": 1, "MovieID": 4, "Rating": 4},
            {"UserID": 2, "MovieID": 1, "Rating": 1},
            {"UserID": 2, "MovieID": 4, "Rating": 4},
        ])


@pytest.fixture
def movies_fixture(spark_fixture):
    yield spark_fixture.createDataFrame(
        schema=datasets['http://files.grouplens.org/datasets/movielens/ml-1m.zip']['ml-1m/movies.dat'],
        data=[
            {"MovieID": 1, "Title": "m1"},
            {"MovieID": 2, "Title": "m2"},
            {"MovieID": 3, "Title": "m3"},
            {"MovieID": 4, "Title": "m4"},
        ])


@pytest.fixture
def expected_ratings_fixture(spark_fixture):
    yield spark_fixture.createDataFrame(
        schema=StructType([
            StructField('MovieID', IntegerType(), True),
            StructField('Title', StringType(), True),
            StructField('Genres', StringType(), True),
            StructField('min(Rating)', IntegerType(), True),
            StructField('max(Rating)', IntegerType(), True),
            StructField('avg(Rating)', DoubleType(), True)
        ]),
        data=[
            {"MovieID": 1, "Title": "m1", "min(Rating)": 1, "max(Rating)": 1, "avg(Rating)": 1.0},
            {"MovieID": 2, "Title": "m2", "min(Rating)": 2, "max(Rating)": 2, "avg(Rating)": 2.0},
            {"MovieID": 3, "Title": "m3", "min(Rating)": 3, "max(Rating)": 3, "avg(Rating)": 3.0},
            {"MovieID": 4, "Title": "m4", "min(Rating)": 4, "max(Rating)": 4, "avg(Rating)": 4.0},
        ]
    )


@pytest.fixture
def expected_top_user_movies(spark_fixture):
    yield spark_fixture.createDataFrame(
        schema=StructType([
            StructField('UserID', IntegerType(), True),
            StructField('MovieID', IntegerType(), True)
        ]),
        data=[
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 1),
            (2, 4),
        ])
