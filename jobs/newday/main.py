import logging
import tempfile
import zipfile
from operator import itemgetter

import pyspark.sql
import wget

from jobs.newday.schema import datasets
from jobs.newday.transformations import compute_movie_ratings, compute_top_user_movies
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

DEFAULT_DATASET_URL = 'http://files.grouplens.org/datasets/movielens/ml-1m.zip'
DEFAULT_DESTINATION = '/tmp/'
DEFAULT_OUTPUT_FORMAT = 'parquet'



def download_dataset(sc: pyspark.SparkContext, url: str, schemas: dict[str, StructType]):
    dataframes = {}
    with tempfile.TemporaryDirectory() as tempdir:
        filename = wget.download(url=url, out=tempdir)
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            for name, schema in schemas.items():
                extracted_file_name = zip_ref.extract(member=name, path=tempdir)
                dataframes[name] = sc.read.csv(extracted_file_name, sep='::', header=False, schema=schema)

    return dataframes


def save_df(df: pyspark.sql.DataFrame, output_format: str, destination: str, name: str):
    logger.info(f'Saving {name} to {destination} in {output_format} format')
    df.show()
    df.write.format(output_format).mode("overwrite").save(path=f"{destination}{name}.{output_format}")


def perform(spark: pyspark.SparkContext, args):
    dataset_url = args.get('dataset-url', DEFAULT_DATASET_URL)
    destination = args.get('destination', DEFAULT_DESTINATION)
    output_format = args.get('output-format', DEFAULT_OUTPUT_FORMAT)

    logger.info(f'Staring {dataset_url}')
    assert dataset_url in datasets, "Unknown dataset url"
    schemas = datasets[dataset_url]
    loaded_datasets = download_dataset(sc=spark, url=dataset_url, schemas=schemas)
    movies, ratings = itemgetter(*schemas.keys())(loaded_datasets)

    movie_ratings = compute_movie_ratings(movies, ratings)
    save_df(df=movie_ratings, destination=destination, output_format=output_format, name="movie_ratings")

    top_user_movies = compute_top_user_movies(ratings)
    save_df(df=top_user_movies, destination=destination, output_format=output_format, name="top_user_movies")
