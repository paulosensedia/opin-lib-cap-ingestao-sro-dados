from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from opin_lib_chassis_dados.config.configuration import Configuration


class AppSparkContext(ABC):
    """
    Classe responsável por fornecer uma abstração de classe contexto Spark, com
    o intuito transparecer a criação de sessões Apache Spark específica para o
    ambiente em que a aplicação é executada.
    """

    def __init__(self):
        self.spark = None

    @abstractmethod
    def create_instance(self, config: Configuration, spark: SparkSession, dbutils):
        pass

    def get_instance(self, config: Configuration, spark: SparkSession, dbutils) -> SparkSession:
        return self.create_instance(config, spark, dbutils)

    @staticmethod
    def is_spark_instanciated(spark: SparkSession) -> bool:
        return spark is not None
