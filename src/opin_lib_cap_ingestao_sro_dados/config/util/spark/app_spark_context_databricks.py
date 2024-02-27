from pyspark.sql import SparkSession

from opin_lib_cap_ingestao_sro_dados.config.configuration import Configuration
from opin_lib_cap_ingestao_sro_dados.config.util.spark.app_spark_context import AppSparkContext


class AppSparkContextDatabricks(AppSparkContext):
    """
    Classe responsável por retornar a sessão Apache Spark originada no
    Databricks aplicando configurações específicas para execução em ambiente
    distribuído.
    """

    def create_instance(self, config: Configuration, spark: SparkSession, dbutils):
        spark.conf.set("fs.azure.account.auth.type", "OAuth")
        spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set("fs.azure.account.oauth2.client.id", dbutils.get_kvault('azu.bsdatalake.applicationid'))
        spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.get_kvault('azu.bsdatalake.authenticationkey'))
        spark.conf.set("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{dbutils.get_kvault('azu.bsdatalake.tenantid')}/oauth2/token")
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
        return spark