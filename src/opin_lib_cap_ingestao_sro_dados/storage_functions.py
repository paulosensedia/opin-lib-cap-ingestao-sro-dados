from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_cap_ingestao_sro_dados.config.util.environment_enum import EnvironmentEnum

OVERWRITE = 'overwrite'


def get_files(dbutils, path):
    return dbutils.fs_ls(path)


def is_empty(dbutils, path: str):
    files = get_files(dbutils, path)
    return 0 == len(files)


def delete(dbutils, path: str, env: Environment):
    if is_empty(dbutils, path):
        return

    files = get_files(dbutils, path)

    if 0 == len(files):
        return

    if env.env_current in (EnvironmentEnum.LOCAL_LINUX, EnvironmentEnum.LOCAL_WIN):
        for file in files:
            dbutils.fs_rm(f"{path}/{file}", True)
    else:
        for file in files:
            dbutils.fs_rm(file.path, True)


def read_text_file(dbutils, spark: SparkSession, file_format: str, has_header: bool,
                   infer_schema: bool, delimiter: str, path: str, charset: str = "UTF-8"):
    if not is_empty(dbutils, path):
        data = spark.read.format(file_format) \
            .option('header', has_header) \
            .option('inferSchema', infer_schema) \
            .option('sep', delimiter) \
            .option("charset", charset) \
            .load(path)

        return data


def read_text_file_from_schema(dbutils, spark: SparkSession, file_format: str, schema: StructType,
                               delimiter: str, path: str, charset: str = 'UTF-8'):
    if not is_empty(dbutils, path):
        data = spark.read.format(file_format) \
            .option('header', False) \
            .option('schema', schema) \
            .option('sep', delimiter) \
            .option('charset', charset) \
            .load(path)

        return data


def read_csv_file(dbutils, spark: SparkSession, delimiter: str, path: str, header: bool, charset: str = 'UTF-8'):
    try:
        if not is_empty(dbutils, path):
            data = spark.read.format('csv') \
                .option('header', header) \
                .option('inferSchema', True) \
                .option('delimiter', delimiter) \
                .option('charset', charset) \
                .load(path)

            return data
    except Exception:
        pass


def read_delta_file(dbutils, spark: SparkSession, path: str):
    if not is_empty(dbutils, path):
        return spark.read \
            .format('delta') \
            .option('inferSchema', 'true') \
            .load(path)


def read_delta_table(spark: SparkSession, path: str):
    try:
        table = DeltaTable.forPath(spark, path)
        return table
    except Exception:
        pass


def write_delta_file(data: DataFrame, path: str, mode: str):
    if not data.rdd.isEmpty():
        data.write \
            .format('delta') \
            .option("overwriteSchema", "true") \
            .save(path, mode=mode)

