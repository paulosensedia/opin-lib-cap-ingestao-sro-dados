# Databricks notebook source
# MAGIC %run /vs-opin-ingestao/opin-lib-ingestao-sro-dados/batch/libs/libs

# COMMAND ----------

# DBTITLE 1,Imports
from opin_lib_ingestao_sro_dados.config.context import Context
from opin_lib_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_ingestao_sro_dados.config.util.environment_enum import EnvironmentEnum
from opin_lib_ingestao_sro_dados.ingestion.ramo_susep import *


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

# DBTITLE 1,Ingestion
ingestion(dbutils,
          spark,
          context.STORAGE_TRANSIENT_INGESTAO_RAMO_SUSEP,
          context.STORAGE_RAW_INGESTAO_RAMO_SUSEP
         )
