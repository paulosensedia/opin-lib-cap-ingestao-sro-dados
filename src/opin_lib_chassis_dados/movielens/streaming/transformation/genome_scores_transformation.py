from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType

from opin_lib_chassis_dados.config.context import Context
from opin_lib_chassis_dados.movielens.enums.enum_genome_score_silver_fields import EnumGenomeScoresSilverFields
from opin_lib_chassis_dados.movielens.streaming.transformation.genome_scores_upsert import UpsertDeltalakeStreaming


def read_delta_stream(spark: SparkSession, format: str, path: str):
    return spark.readStream.format(format).load(path)


def read_delta_table_genome_scores(context: Context):
    files = context.dbutils.fs_ls(context.STORAGE_SILVER_GENOME_SCORES_STREAM)
    if 0 == len(files):
        return DeltaTable \
            .create(context.spark) \
            .addColumn(EnumGenomeScoresSilverFields.IDTFD_FILME.value, StringType()) \
            .addColumn(EnumGenomeScoresSilverFields.IDTFD_MRCAO.value, StringType()) \
            .addColumn(EnumGenomeScoresSilverFields.RELEV.value, StringType()) \
            .location(context.STORAGE_SILVER_GENOME_SCORES_STREAM) \
            .execute()
    return DeltaTable.forPath(context.spark, context.STORAGE_SILVER_GENOME_SCORES_STREAM)


def upsert(context: Context, df: DataFrame, upsert_function: UpsertDeltalakeStreaming):
    df.writeStream \
        .format("delta") \
        .foreachBatch(upsert_function.upsert_to_delta) \
        .outputMode("update") \
        .option("checkpointLocation", context.STORAGE_SILVER_GENOME_SCORES_STREAM_CHECKPOINT) \
        .start() \
        .awaitTermination()
