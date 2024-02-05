# Databricks notebook source
# DBTITLE 1,Get parameter from ADF
try:
    delta_path = dbutils.widgets.get('delta_path')
except:
    delta_path = ""

# COMMAND ----------

# DBTITLE 1,Get Last Request ID from ADF path
try:
    df = spark.read.format("delta").load(delta_path)
    last_request_id = df.agg({"ZCHREQUID": "max"}).collect()[0]["max(ZCHREQUID)"]
except:
    last_request_id = None

# COMMAND ----------

# DBTITLE 1,Return to ADF
dbutils.notebook.exit(last_request_id)
