from pyspark.sql.types import StructType, StructField, IntegerType, StringType

datasets = {
    'http://files.grouplens.org/datasets/movielens/ml-1m.zip': {
        # MovieID::Title::Genres
        'ml-1m/movies.dat': StructType([
            StructField("MovieID", IntegerType(), True),
            StructField("Title", StringType(), True),
            StructField("Genres", StringType(), True)
        ]),
        # UserID::MovieID::Rating::Timestamp
        'ml-1m/ratings.dat': StructType([
            StructField("UserID", IntegerType(), True),
            StructField("MovieID", IntegerType(), True),
            StructField("Rating", IntegerType(), True),
            StructField("Timestamp", IntegerType(), True)
        ])
    }}
