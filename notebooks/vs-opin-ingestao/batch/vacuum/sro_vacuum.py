# Databricks notebook source
%sh pip uninstall -y opin-lib-ingestao-sro-dados

import os
if os.getenv("AMBIENTE"):
    os.system("pip install /dbfs/FileStore/jars/commons/jproperties-2.1.1-py2.py3-none-any.whl")
    os.system("pip install /dbfs/FileStore/jars/vs-opin-ingestao/opin-lib-cap-ingestao-sro-dados/opin_lib_cap_ingestao_sro_dados-1.0.0-py3-none-any.whl")

# COMMAND ----------
# DBTITLE 1,Imports
from opin_lib_cap_ingestao_sro_dados.config.context import Context
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_cap_ingestao_sro_dados.config.util.environment_enum import EnvironmentEnum
from opin_lib_cap_ingestao_sro_dados.functions.vacuum.vacuum_functions import vacuum_delta

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


## Serie
for p in dbutils.fs_ls(context.STORAGE_RAW_INGESTAO_SRO+'/capitalizacao/serie'):
    path_list.append(p.path.replace('dbfs:',''))

## Titulo
for s in dbutils.fs_ls(context.STORAGE_RAW_INGESTAO_SRO+'/capitalizacao/titulo'):
    path_list.append(s.path.replace('dbfs:',''))

# COMMAND ----------

# DBTITLE 1,Vacuum
path_log = '/mnt/bronze/vacuum_logs'

for path in range(len(path_list)):
    vacuum_delta(dbutils, spark, path_list[path], path_log)
