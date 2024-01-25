# Databricks notebook source
dbutils.fs.ls("/mnt/silver/cadastral/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta.`/mnt/silver/test/cdfteste`(
# MAGIC   NDOCTO   string,
# MAGIC   datetime    TIMESTAMP,
# MAGIC   DESC      STRING
# MAGIC   )
# MAGIC USING DELTA LOCATION "/mnt/silver/test/cdfteste"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO delta.`/mnt/silver/test/cdfteste` values
# MAGIC (
# MAGIC '123',
# MAGIC current_timestamp(),
# MAGIC 'TESTE123'
# MAGIC );
# MAGIC INSERT INTO delta.`/mnt/silver/test/cdfteste` values
# MAGIC (
# MAGIC '123',
# MAGIC current_timestamp(),
# MAGIC 'TESTE123'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO delta.`/mnt/silver/test/cdfteste` values
# MAGIC (
# MAGIC '123',
# MAGIC current_timestamp(),
# MAGIC 'TESTE123'
# MAGIC ),
# MAGIC (
# MAGIC '123',
# MAGIC current_timestamp(),
# MAGIC 'TESTE123'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/mnt/silver/test/cdfteste` 
# MAGIC SET DESC = 'NOVADESC2'
# MAGIC WHERE NDOCTO = '1235'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/mnt/silver/test/cdfteste`

# COMMAND ----------

stream_df = spark.readStream.format('delta').option("ignoreChanges",True).load(('/mnt/silver/test/cdfteste'))

# COMMAND ----------

dbutils.fs.ls('/mnt/silver/test/cdfteste/_change_data')

# COMMAND ----------

display(stream_df)

# COMMAND ----------

stream_cdf = spark.readStream.format('delta').option("readChangeFeed", "true").option("startingVersion", 3).load('/mnt/silver/test/cdfteste')

# COMMAND ----------

display(stream_cdf)

# COMMAND ----------

stream_cdf_timestamp = spark.readStream.format('delta').option("readChangeFeed", "true").option("startingTimestamp", "2022-09-19 14:00:00").load('/mnt/silver/test/cdfteste')

# COMMAND ----------

display(stream_cdf_timestamp)

# COMMAND ----------

batch_cdf = spark.read.format('delta').option("readChangeFeed", "true").option("startingVersion", 3).option("endingVersion", 11).load('/mnt/silver/test/cdfteste')

# COMMAND ----------

display(batch_cdf)

# COMMAND ----------

batch_cdf_timestamp = spark.read.format('delta').option("readChangeFeed", "true").option("startingTimestamp", "2022-09-19 14:00:00").option("endingTimestamp", '2022-09-21 12:00:00').load('/mnt/silver/test/cdfteste')

# COMMAND ----------

display(batch_cdf_timestamp)

# COMMAND ----------


