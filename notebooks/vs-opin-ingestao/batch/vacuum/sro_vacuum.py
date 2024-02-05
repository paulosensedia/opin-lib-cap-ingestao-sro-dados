# Databricks notebook source
# MAGIC %sh pip uninstall -y opin-lib-ingestao-sro-dados

# COMMAND ----------
# MAGIC %run /vs-opin-ingestao/opin-lib-ingestao-sro-dados/batch/libs/libs

# COMMAND ----------
# DBTITLE 1,Imports
from opin_lib_ingestao_sro_dados.config.context import Context
from opin_lib_ingestao_sro_dados.vacuum.vacuum_functions import *
from opin_lib_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_ingestao_sro_dados.config.util.environment_enum import EnvironmentEnum


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

# DBTITLE 1,Paths
path_list = []

## Ramo Susep
path_list.append(context.STORAGE_RAW_INGESTAO_RAMO_SUSEP)

## Premio
for p in dbutils.fs_ls(context.STORAGE_RAW_INGESTAO_SRO+'/premio'):
    path_list.append(p.path.replace('dbfs:',''))

## Sinistro
for s in dbutils.fs_ls(context.STORAGE_RAW_INGESTAO_SRO+'/sinistro'):
    path_list.append(s.path.replace('dbfs:',''))


# COMMAND ----------

# DBTITLE 1,Vacuum
path_log = '/mnt/bronze/vacuum_logs'

for path in range(len(path_list)):
    vacuum_delta(dbutils, spark, path_list[path], path_log)
