from pyspark.sql import SparkSession

from opin_lib_cap_ingestao_sro_dados.config.configuration import Configuration
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_cap_ingestao_sro_dados.config.util.spark.app_spark_context_databricks import AppSparkContextDatabricks
from opin_lib_cap_ingestao_sro_dados.config.util.spark.app_spark_context_local import AppSparkContextLocal
from opin_lib_cap_ingestao_sro_dados.config.util.environment_enum import EnvironmentEnum

class SparkContextHelper():
    @staticmethod
    def get_instance(env: Environment, conf: Configuration, spark, dbutils) -> SparkSession:
        if env.env_current in (EnvironmentEnum.LOCAL_WIN, EnvironmentEnum.LOCAL_LINUX):
            return AppSparkContextLocal().get_instance(conf, spark, dbutils)
        return AppSparkContextDatabricks().get_instance(conf, spark, dbutils)
