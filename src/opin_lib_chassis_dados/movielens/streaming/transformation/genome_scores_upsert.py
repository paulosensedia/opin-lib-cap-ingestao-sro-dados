from delta import DeltaTable
from pyspark.sql import DataFrame

from opin_lib_chassis_dados.movielens.enums.genome_score_streaming_ingestion_fields import EnumGenomeScoresBronzeFields
from opin_lib_chassis_dados.movielens.enums.enum_genome_score_silver_fields import EnumGenomeScoresSilverFields


class UpsertDeltalakeStreaming():


    def __init__(self, df_delta: DeltaTable):
        self.df_delta = df_delta

        self.dict_upsert = {
            f"{EnumGenomeScoresSilverFields.IDTFD_FILME.value}": f"df.{EnumGenomeScoresBronzeFields.MOVIE_ID.value}",
            f"{EnumGenomeScoresSilverFields.IDTFD_MRCAO.value}": f"df.{EnumGenomeScoresBronzeFields.TAG_ID.value}",
            f"{EnumGenomeScoresSilverFields.RELEV.value}": f"df.{EnumGenomeScoresBronzeFields.RELEVANCE.value}"
        }

    def upsert_to_delta(self, df_stream: DataFrame, batch_id: int):
        self.df_delta.alias("df_delta") \
            .merge(df_stream.alias("df"),
                f"df_delta.{EnumGenomeScoresSilverFields.IDTFD_FILME.value} = df.{EnumGenomeScoresBronzeFields.MOVIE_ID.value} AND "
                f"df_delta.{EnumGenomeScoresSilverFields.IDTFD_MRCAO.value} = df.{EnumGenomeScoresBronzeFields.TAG_ID.value}"
            ) \
            .whenNotMatchedInsert(values=self.dict_upsert) \
            .whenMatchedUpdate(condition=f"df.{EnumGenomeScoresBronzeFields.DATABASE_OPERATION_INDICATOR.value} = 'U'", set=self.dict_upsert) \
            .whenMatchedDelete(condition=f"df.{EnumGenomeScoresBronzeFields.DATABASE_OPERATION_INDICATOR.value} = 'D'") \
            .execute()
