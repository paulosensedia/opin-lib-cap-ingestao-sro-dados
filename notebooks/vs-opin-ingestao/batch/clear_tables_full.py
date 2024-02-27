# Databricks notebook source
# MAGIC %sh pip uninstall -y opin-lib-ingestao-sro-dados

# COMMAND ----------

# MAGIC %run /vs-opin-ingestao/opin-lib-ingestao-sro-dados/batch/libs/libs

# COMMAND ----------
from opin_lib_cap_ingestao_sro_dados.config.context import Context
from opin_lib_cap_ingestao_sro_dados.ingestion.openhub_full import *
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_cap_ingestao_sro_dados.config.util.environment_enum import EnvironmentEnum

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

tables = get_tables_name_full(dbutils, spark, context.STORAGE_TRANSIENT_INGESTAO_TABLES_FULL + '.csv')

for item in tables.collect():
    path = context.STORAGE_RAW_INGESTAO_SRO + '/' + item.path_sink

    try:
        print(path)
        dbutils.fs.rm(path, recurse=True)
    except Exception:
        pass
