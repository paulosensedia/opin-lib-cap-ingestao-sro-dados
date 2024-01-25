from pyspark.sql.types import StructField, StructType, StringType
from opin_lib_chassis_dados.movielens.enums.enum_ratings_bronze_fields import EnumRatingsBronzeFields


class RatingsSchemaIngestion():

    @staticmethod
    def schema() -> StructType:
        return StructType([
            StructField(EnumRatingsBronzeFields.USER_ID.value, StringType(), True),
            StructField(EnumRatingsBronzeFields.MOVIE_ID.value, StringType(), True),
            StructField(EnumRatingsBronzeFields.RATING.value, StringType(), True),
            StructField(EnumRatingsBronzeFields.TIMESTAMP.value, StringType(), True),
            StructField(EnumRatingsBronzeFields.DATABASE_OPERATION_INDICATOR.value, StringType(), True)
        ])
