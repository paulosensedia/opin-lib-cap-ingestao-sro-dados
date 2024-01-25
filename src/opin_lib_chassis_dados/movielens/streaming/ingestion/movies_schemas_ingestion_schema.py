from pyspark.sql.types import StructField, StructType, StringType
from opin_lib_chassis_dados.movielens.enums.enum_movies_bronze_fields import EnumMoviesBronzeFields


class MoviesSchemaIngestion():

    @staticmethod
    def schema() -> StructType:
        return StructType([
            StructField(EnumMoviesBronzeFields.MOVIE_ID.value, StringType(), True),
            StructField(EnumMoviesBronzeFields.TITLE.value, StringType(), True),
            StructField(EnumMoviesBronzeFields.GENRES.value, StringType(), True),
            StructField(EnumMoviesBronzeFields.DATABASE_OPERATION_INDICATOR.value, StringType(), True)
        ])
