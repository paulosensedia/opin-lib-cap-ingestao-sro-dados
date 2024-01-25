from pyspark.sql.streaming import DataStreamReader

from opin_lib_testes_conexoes.config.context import Context
from opin_lib_testes_conexoes.config.util.kafka.kafka_connect import KafkaConnect


class KafkaConnectDatabricks(KafkaConnect):
    """
    Classe responsável por retornar um reader com as configurações de ambiente
    Databricks.
    """

    def get_connection(self, context: Context) -> DataStreamReader:

        data_stream_reader = context.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", context.KAFKA_BOOTSTRAP_SERVERS) \
            .option("kafka.security.protocol", context.KAFKA_SECURITY_PROTOCOL) \
            .option("kafka.sasl.jaas.config", context.KAFKA_SASL_JAAS_CONFIG) \
            .option("kafka.ssl.endpoint.identification.algorithm", context.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM) \
            .option("kafka.sasl.mechanism", context.KAFKA_SASL_MECHANISM)

        return data_stream_reader
