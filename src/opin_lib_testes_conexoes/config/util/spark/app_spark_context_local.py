from pyspark.sql import SparkSession

from opin_lib_testes_conexoes.config.configuration import Configuration
from opin_lib_testes_conexoes.config.util.spark.app_spark_context import AppSparkContext


class AppSparkContextLocal(AppSparkContext):
    """
    Classe responsável por instanciar uma sessão Apache Spark para execução em
    ambiente local.
    """

    def create_instance(self, config: Configuration, spark: SparkSession, dbutils):
        if AppSparkContext.is_spark_instanciated(spark):
            return spark

        spark_packages_list = ['io.delta:delta-core_2.12:1.1.0', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1', 'org.apache.spark:spark-avro_2.12:3.2.1']
        spark_packages = ",".join(spark_packages_list)

        SPARK_SQL_WAREHOUSE_DIR = config.get("spark.sql.warehouse.dir")
        return SparkSession \
            .builder \
            .config("spark.jars.packages", spark_packages) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.shuffle.service.enabled", False) \
            .config("spark.dynamicAllocation.enabled", False) \
            .config("spark.sql.repl.eagerEval.enabled", True) \
            .config("spark.sql.warehouse.dir", SPARK_SQL_WAREHOUSE_DIR) \
            .config("hive.stats.jdbc.timeout", 30) \
            .config("hive.stats.retries.wait", 3000) \
            .config("spark.streaming.stopGracefullyOnShutdown", True) \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", False) \
            .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName={0};create=true".format(SPARK_SQL_WAREHOUSE_DIR)) \
            .appName("local-notebook") \
            .enableHiveSupport() \
            .getOrCreate()
