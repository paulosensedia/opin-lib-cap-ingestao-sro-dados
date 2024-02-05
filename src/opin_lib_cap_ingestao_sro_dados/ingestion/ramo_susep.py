from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from unicodedata import normalize

from opin_lib_cap_ingestao_sro_dados.storage_functions import write_delta_file, read_csv_file


def normalize_accented_word(value):
    target = normalize('NFKD', value).encode('ASCII', 'ignore').decode('ASCII')
    return target


def upper_columns(df):
    df = df.select("*", f.upper(f.col('RRAMO'))).drop('RRAMO')
    df = df.select("*", f.upper(f.col('RGRP_RAMO'))).drop('RGRP_RAMO')
    df = df.select(f.col("CRAMO").alias("CRAMO"), f.col("upper(RRAMO)").alias("RRAMO"),
                   f.col("upper(RGRP_RAMO)").alias("RGRP_RAMO"), f.col("ZCHCIAPRO_PARM"))
    return df


def normalize_accented(df):
    udf_normalize_accented_word = udf(lambda x: normalize_accented_word(x), StringType())
    df = df.withColumn("RGRP_RAMO", udf_normalize_accented_word(f.col("RGRP_RAMO")))
    return df


def ingestion(dbutils, spark, input_path, output_path):
    df = read_csv_file(dbutils, spark, '|', input_path, True, 'iso-8859-1')
    df = upper_columns(df)
    df = normalize_accented(df)
    write_delta_file(df, output_path, 'overwrite')
