# Databricks notebook source
# MAGIC %run /vs-opin-ingestao/opin-lib-ingestao-sro-dados/batch/libs/libs


# COMMAND ----------
from opin_lib_ingestao_sro_dados.config.context import Context
from opin_lib_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_ingestao_sro_dados.config.util.environment_enum import EnvironmentEnum
from opin_lib_ingestao_sro_dados.ingestion.openhub_full import *
import pyspark.sql.functions as f
from opin_lib_ingestao_sro_dados.storage_functions import write_delta_file

# COMMAND ----------
# DBTITLE 1,Initialize
env = Environment()
if env.env_current in (EnvironmentEnum.LOCAL_LINUX, EnvironmentEnum.LOCAL_WIN):
    dbutils = None
    spark = None

context = Context(spark, env, dbutils)
spark = context.spark
dbutils = context.dbutils

# COMMAND ----------

path = "/mnt/bronze/ingestao_dados/sro/premio/ZDAOPIN05"
df = spark.read.format("delta").load(path)
df = df.withColumn('ZCHSININT', f.lit(""))

write_delta_file(df, path, 'overwrite')

# COMMAND ----------

df_read = spark.read.format("delta").load(path)

print('Total de registros: '+str(df_read.count()))
print('Total de registros vazios: '+str(df_read.where('ZCHSININT == ""').count()))
print('Total de registros com "0": '+str(df_read.where('ZCHSININT == 0').count()))
df_read.display()
