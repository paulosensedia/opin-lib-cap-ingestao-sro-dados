from pyspark.sql.types import StructField, StructType, StringType
from opin_lib_chassis_dados.movielens.enums.enum_tags_bronze_fields import EnumTagsBronzeFields


class TagsSchemaIngestion():

    @staticmethod
    def schema() -> StructType:
        return StructType([
            StructField(EnumTagsBronzeFields.USER_ID.value, StringType(), True),
            StructField(EnumTagsBronzeFields.MOVIE_ID.value, StringType(), True),
            StructField(EnumTagsBronzeFields.TAG.value, StringType(), True),
            StructField(EnumTagsBronzeFields.TIMESTAMP.value, StringType(), True),
            StructField(EnumTagsBronzeFields.DATABASE_OPERATION_INDICATOR.value, StringType(), True)
        ])
