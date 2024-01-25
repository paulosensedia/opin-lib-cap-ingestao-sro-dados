from pyspark.sql.streaming import DataStreamReader

from opin_lib_testes_conexoes.config.context import Context
from opin_lib_testes_conexoes.config.util.kafka.kafka_connect_databricks import KafkaConnectDatabricks
from opin_lib_testes_conexoes.config.util.kafka.kafka_connect_local import KafkaConnectLocal
from opin_lib_testes_conexoes.config.util.environment_enum import EnvironmentEnum


class KafkaConnectHelper:
    """
    Classe auxiliar para obter uma conexão a partir do ambiente em que o
    processo é executado.
    """

    @staticmethod
    def get_connection(context: Context) -> DataStreamReader:
        if context.env.env_current in (
        EnvironmentEnum.LOCAL_WIN, EnvironmentEnum.LOCAL_LINUX):
            return KafkaConnectLocal().get_connection(context)
        return KafkaConnectDatabricks().get_connection(context)
