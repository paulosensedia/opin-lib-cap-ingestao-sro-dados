from pyspark.sql.types import StructField, StructType, StringType

from opin_lib_chassis_dados.movielens.enums.enum_links_bronze_fields import EnumLinksBronzeFields


class LinksSchemaIngestion():

    @staticmethod
    def schema() -> StructType:
        return StructType([
            StructField(EnumLinksBronzeFields.MOVIE_ID.value, StringType(), True),
            StructField(EnumLinksBronzeFields.IMDB_ID.value, StringType(), True),
            StructField(EnumLinksBronzeFields.TMDB_ID.value, StringType(), True),
            StructField(EnumLinksBronzeFields.DATABASE_OPERATION_INDICATOR.value, StringType(), True)
        ])
