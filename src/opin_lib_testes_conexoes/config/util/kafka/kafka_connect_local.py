from pyspark.sql.streaming import DataStreamReader

from opin_lib_testes_conexoes.config.context import Context
from opin_lib_testes_conexoes.config.util.kafka.kafka_connect import KafkaConnect


class KafkaConnectLocal(KafkaConnect):
    """
    Classe responsável por retornar um reader com as configurações de ambiente
    local.
    """

    def get_connection(self, context: Context) -> DataStreamReader:
        data_stream_reader = context.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", context.KAFKA_BOOTSTRAP_SERVERS)
        return data_stream_reader