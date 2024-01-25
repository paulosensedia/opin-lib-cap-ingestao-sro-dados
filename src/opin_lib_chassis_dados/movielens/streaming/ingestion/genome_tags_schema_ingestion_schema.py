from pyspark.sql.types import StructField, StructType, StringType

from opin_lib_chassis_dados.movielens.enums.enum_genome_tags_bronze_fields import EnumGenomeTagsBronzeFields


class GenomeTagsSchemaIngestion():

    @staticmethod
    def schema() -> StructType:
        return StructType([
            StructField(EnumGenomeTagsBronzeFields.TAG_ID.value, StringType(), True),
            StructField(EnumGenomeTagsBronzeFields.TAG.value, StringType(), True),
            StructField(EnumGenomeTagsBronzeFields.DATABASE_OPERATION_INDICATOR.value, StringType(), True)
        ])
