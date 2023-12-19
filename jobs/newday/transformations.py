import pyspark.sql
from pyspark.sql import Window
from pyspark.sql.functions import min, max, avg, row_number


def compute_movie_ratings(movies: pyspark.sql.DataFrame, ratings: pyspark.sql.DataFrame):
    movies_and_ratings = movies.join(ratings, ['MovieID'], 'left')
    aggregations = [
        min(ratings.Rating),
        max(ratings.Rating),
        avg(ratings.Rating)
    ]
    return movies_and_ratings.groupBy(movies.MovieID, movies.Title, movies.Genres).agg(*aggregations)


def compute_top_user_movies(ratings: pyspark.sql.DataFrame, number_of_movies=3):
    window_group_by_columns = Window.partitionBy([ratings.UserID])
    row_number_col = row_number().over(window_group_by_columns.orderBy(ratings.Rating.desc())).alias('row_rank')
    ordered_df = ratings.select(ratings.columns + [row_number_col])
    return ordered_df.filter(f"row_rank <= {number_of_movies}").drop(row_number_col).select(ordered_df.UserID, ordered_df.MovieID)
