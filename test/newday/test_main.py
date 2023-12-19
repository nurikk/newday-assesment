from unittest.mock import MagicMock, patch, call, ANY

import pytest
from pyspark.sql import DataFrame
from pyspark.testing import assertDataFrameEqual

from jobs.newday.main import save_df, perform


@pytest.mark.parametrize("destination, output_format, name", [
    ("", "csv", "fpp"),
    ("/tmp/foo/bar", "csv", "fpp"),

    ("/tmp/foo/bar", "parquet", "name"),
    ("/tmp/foo/bar", "parquet", "name"),

    ("s3://bucket/", "parquet", "name"),
    ("s3://bucket/", "parquet", "name"),
])
def test_save_df(destination, output_format, name):
    mock_df = MagicMock()
    save_df(df=mock_df, destination=destination, output_format=output_format, name=name)
    mock_df.write.format.assert_called_with(output_format)
    mock_df.write.format.return_value.mode.assert_called_with("overwrite")
    mock_df.write.format.return_value.mode.return_value.save.assert_called_with(path=f'{destination}{name}.{output_format}')


@patch("jobs.newday.main.save_df")
@patch("jobs.newday.main.download_dataset")
def test_perform(mock_download_dataset: MagicMock, mock_save_df: MagicMock,
                 spark_fixture,
                 movies_fixture,
                 ratings_fixture,
                 expected_ratings_fixture,
                 expected_top_user_movies):
    mock_args = {}
    mock_download_dataset.return_value = {
        'ml-1m/movies.dat': movies_fixture,
        'ml-1m/ratings.dat': ratings_fixture
    }
    perform(spark=spark_fixture, args=mock_args)

    mock_save_df.assert_has_calls([
        call(df=ANY, destination='/tmp/', output_format='parquet', name='movie_ratings'),
        call(df=ANY, destination='/tmp/', output_format='parquet', name='top_user_movies')
    ])

    assertDataFrameEqual(mock_save_df.mock_calls[0].kwargs['df'], expected_ratings_fixture)
    assertDataFrameEqual(mock_save_df.mock_calls[1].kwargs['df'], expected_top_user_movies)
