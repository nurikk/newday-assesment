from jobs.newday.transformations import compute_movie_ratings, compute_top_user_movies
from pyspark.testing.utils import assertDataFrameEqual


def test_compute_top_user_movies(spark_fixture, ratings_fixture, expected_top_user_movies):
    actual = compute_top_user_movies(ratings=ratings_fixture, number_of_movies=3)
    assertDataFrameEqual(actual, expected_top_user_movies)


def test_compute_movie_ratings(spark_fixture, ratings_fixture, movies_fixture, expected_ratings_fixture):
    actual = compute_movie_ratings(movies=movies_fixture, ratings=ratings_fixture)
    assertDataFrameEqual(actual, expected_ratings_fixture)
