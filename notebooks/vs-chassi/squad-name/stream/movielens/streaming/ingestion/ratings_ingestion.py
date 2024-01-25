# COMMAND ----------
# DBTITLE 1,Imports
from opin_lib_chassis_dados.config.context import Context
from opin_lib_chassis_dados.config.util.environment import Environment
from opin_lib_chassis_dados.config.util.environment_enum import EnvironmentEnum
from opin_lib_chassis_dados.config.util.kafka.kafka_connect_helper import KafkaConnectHelper
from opin_lib_chassis_dados.functions.dataframe import DataFrameFunctions

from opin_lib_chassis_dados.functions.deltalake import DeltaLakeFunctions
from opin_lib_chassis_dados.movielens.enums.enum_format_types import EnumFormatTypes
from opin_lib_chassis_dados.movielens.streaming.ingestion.ratings_schemas_ingestion_schema import RatingsSchemaIngestion


# COMMAND ----------
# DBTITLE 1,Initialize context
env = Environment()
if env.env_current in (EnvironmentEnum.LOCAL_LINUX, EnvironmentEnum.LOCAL_WIN):
    dbutils = None
    spark = None

context = Context(spark, env, dbutils)


# COMMAND ----------
# DBTITLE 1,Read data from Kafka topic
kafka_connection = KafkaConnectHelper.get_connection(context)
ratings_topic = kafka_connection \
    .option("subscribe", "MOVIELENS_RATINGS") \
    .option("startingOffsets", "latest") \
    .load()


# COMMAND ----------
# DBTITLE 1,Convert data from json
schema = RatingsSchemaIngestion.schema()
df_ratings = DataFrameFunctions.parse_json(ratings_topic, "value", "json_data", schema).select("json_data.*")


# COMMAND ----------
# DBTITLE 1,Write to bronze layer
DeltaLakeFunctions.write_delta_stream(df_ratings, EnumFormatTypes.DELTA.value,
    context.STORAGE_BRONZE_RATINGS_STREAM,
    context.STORAGE_BRONZE_RATINGS_STREAM_CHECKPOINT)
