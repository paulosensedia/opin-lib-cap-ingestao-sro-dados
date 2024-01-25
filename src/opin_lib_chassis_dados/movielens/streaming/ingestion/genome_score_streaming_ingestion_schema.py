from pyspark.sql.types import StructField, StructType, StringType

from opin_lib_chassis_dados.movielens.enums.genome_score_streaming_ingestion_fields import EnumGenomeScoresBronzeFields


class GenomeScoresSchemaIngestion():

    @staticmethod
    def schema() -> StructType:
        return StructType([
            StructField(EnumGenomeScoresBronzeFields.MOVIE_ID.value, StringType(), True),
            StructField(EnumGenomeScoresBronzeFields.TAG_ID.value, StringType(), True),
            StructField(EnumGenomeScoresBronzeFields.RELEVANCE.value, StringType(), True),
            StructField(EnumGenomeScoresBronzeFields.DATABASE_OPERATION_INDICATOR.value, StringType(), True)
        ])
