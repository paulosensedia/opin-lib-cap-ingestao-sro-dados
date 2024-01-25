from pyspark.sql import SparkSession

from opin_lib_testes_conexoes.config.util.dbutils.dbutils_helper import DBUtilsHelper
from opin_lib_testes_conexoes.config.util.logger import Logger
from opin_lib_testes_conexoes.config.configuration import Configuration
from opin_lib_testes_conexoes.config.util.environment import Environment
from opin_lib_testes_conexoes.config.util.spark.app_spark_context_helper import SparkContextHelper


class Context:
    """
    O objetivo dessa classe Context é iniciar o contexto da aplicação de acordo
    com o ambiente em que é executado. Logo, torna-se um ponto comum para
    acesso a recursos externos e específicos de cada ambiente.
    """

    _LOG = Logger(path=__name__, name=__qualname__)
    _LOG.info("Initializing application context.")

    def __init__(self, spark: SparkSession, env: Environment, dbutils):
        self.env = env
        self.config = Configuration(self.env)
        self.dbutils = DBUtilsHelper().get_instance(self.env, self.config, dbutils)
        self.spark = SparkContextHelper().get_instance(self.env, self.config, spark, self.dbutils)

        self._LOG.info("Default configuration: {0}".format(self.env.env_default.value))
        self._LOG.info("Current configuration: {0}".format(self.env.env_current.value))
        self._LOG.info("PySpark Version: {0} ".format(self.spark.version))
        self.load_properties()

    def load_properties(self):
        # KAFKA
        self.KAFKA_BOOTSTRAP_SERVERS = self.config.get("kafka.bootstrap.servers")
        self.KAFKA_SASL_JAAS_CONFIG = self.config.get("kafka.sasl.jaas.config")
        self.KAFKA_SASL_MECHANISM = self.config.get("kafka.sasl.mechanism")
        self.KAFKA_SECURITY_PROTOCOL = self.config.get("kafka.security.protocol")
        self.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = self.config.get("kafka.ssl.endpoint.identification.algorithm")
        self.KAFKA_TOPIC_DADOS_COMPLEMENTARES_PF = self.config.get("kafka.topic.name.dados.complementares.pf")
        self.KAFKA_TOPIC_DADOS_COMPLEMENTARES_PJ = self.config.get("kafka.topic.name.dados.complementares.pj")
        self.KAFKA_TOPIC_CONSENTIMENTO = self.config.get("kafka.topic.name.consent")
        self.KAFKA_TOPIC_REPRESENTANTE_LEGAL = self.config.get("kafka.topic.name.legal.representative")

        # STORAGE
        self.STORAGE_MOUNT_PATH = self.config.get("storage.mount.path")
        self.STORAGE_MOUNT_TRANSIENT = f"/dbfs{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.transient')}"
        self.STORAGE_MOUNT_BRONZE = f"/dbfs{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.bronze')}"
        self.STORAGE_MOUNT_SILVER = f"/dbfs{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.silver')}"
        self.STORAGE_MOUNT_GOLD = f"/dbfs{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.gold')}"

        # COSMOSDB:
        self.COSMOSDB_URI_RE_TRANSACIONAL = self.dbutils.get_kvault_cosmosdb('cosmosdb.uri.re.transacional')
        self.COSMOSDB_URI_RE_PRODUTOS = self.dbutils.get_kvault_cosmosdb('cosmosdb.uri.re.produtos')
        self.COSMOSDB_URI_CADASTRAL = self.dbutils.get_kvault_cosmosdb('cosmosdb.uri.cadastral')
        self.COSMOSDB_URI_DADOS_COMPLEMENTARES = self.dbutils.get_kvault_cosmosdb('cosmosdb.uri.dados.complementares')
        self.COSMOSDB_URI_REPRESENTANTE_LEGAL = self.dbutils.get_kvault_cosmosdb('cosmosdb.uri.dados.representante.legal')
        self.COSMOSDB_URI_AUTO = self.dbutils.get_kvault_cosmosdb('cosmosdb.uri.auto')
        self.COSMOSDB_URI_AUTO_TRANSACIONAL = self.dbutils.get_kvault_cosmosdb('cosmosdb.uri.auto.transacional')