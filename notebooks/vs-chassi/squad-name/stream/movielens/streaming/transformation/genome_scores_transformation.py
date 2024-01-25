# COMMAND ----------
# DBTITLE 1,Imports
from delta import DeltaTable
from pyspark.sql.types import StringType

from opin_lib_chassis_dados.config.context import Context
from opin_lib_chassis_dados.config.util.environment import Environment
from opin_lib_chassis_dados.config.util.environment_enum import EnvironmentEnum


# COMMAND ----------
# DBTITLE 1,Initialize context
from opin_lib_chassis_dados.movielens.enums.enum_format_types import EnumFormatTypes
from opin_lib_chassis_dados.movielens.streaming.transformation.genome_scores_transformation import read_delta_table_genome_scores, read_delta_stream, upsert
from opin_lib_chassis_dados.movielens.streaming.transformation.genome_scores_upsert import UpsertDeltalakeStreaming

env = Environment()
if env.env_current in (EnvironmentEnum.LOCAL_LINUX, EnvironmentEnum.LOCAL_WIN):
    dbutils = None
    spark = None

context = Context(spark, env, dbutils)


# COMMAND ----------
# DBTITLE 1,Read data streaming from storage
df = read_delta_stream(context.spark, EnumFormatTypes.DELTA.value, context.STORAGE_BRONZE_GENOME_SCORES_STREAM)


# COMMAND ----------
# DBTITLE 1,Read delta table
df_delta = read_delta_table_genome_scores(context)


# COMMAND ----------
# DBTITLE 1,Upsert
upsert_streaming = UpsertDeltalakeStreaming(df_delta)
upsert(context, df, upsert_streaming)



