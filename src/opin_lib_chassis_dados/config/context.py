from pyspark.sql import SparkSession

from opin_lib_chassis_dados.config.util.dbutils.dbutils_helper import DBUtilsHelper
from opin_lib_chassis_dados.config.util.logger import Logger
from opin_lib_chassis_dados.config.configuration import Configuration
from opin_lib_chassis_dados.config.util.environment import Environment
from opin_lib_chassis_dados.config.util.spark.app_spark_context_helper import SparkContextHelper


class Context:
    """
    O objetivo dessa classe Context é iniciar o contexto da aplicação de acordo
    com o ambiente em que é executado. Logo, torna-se um ponto comum para
    acesso a recursos externos e específicos de cada ambiente.
    """

    _LOG = Logger(path=__name__, name=__qualname__)
    _LOG.info("Initializing application context.")

    EMPTY = ''

    KAFKA_BOOTSTRAP_SERVERS = EMPTY
    KAFKA_SASL_JAAS_CONFIG = EMPTY
    KAFKA_SASL_MECHANISM = EMPTY
    KAFKA_SECURITY_PROTOCOL = EMPTY
    KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = EMPTY

    # STORAGE
    STORAGE_TMP_PATH = EMPTY
    STORAGE_MOUNT_PATH = EMPTY

    # STORAGE EXTERNAL
    STORAGE_MOUNT_EXTERNAL = EMPTY
    STORAGE_EXTERNAL_GENOME_SCORES = EMPTY
    STORAGE_EXTERNAL_GENOME_TAGS = EMPTY
    STORAGE_EXTERNAL_LINKS = EMPTY
    STORAGE_EXTERNAL_MOVIES = EMPTY
    STORAGE_EXTERNAL_RATINGS = EMPTY
    STORAGE_EXTERNAL_TAGS = EMPTY

    # STORAGE TRANSIENT
    STORAGE_MOUNT_TRANSIENT = EMPTY
    STORAGE_TRANSIENT_GENOME_SCORES = EMPTY
    STORAGE_TRANSIENT_GENOME_TAGS = EMPTY
    STORAGE_TRANSIENT_LINKS = EMPTY
    STORAGE_TRANSIENT_MOVIES = EMPTY
    STORAGE_TRANSIENT_RATINGS = EMPTY
    STORAGE_TRANSIENT_TAGS = EMPTY

    # STORAGE BRONZE
    STORAGE_MOUNT_BRONZE = EMPTY
    STORAGE_BRONZE_GENOME_SCORES_STREAM = EMPTY
    STORAGE_BRONZE_GENOME_TAGS_STREAM = EMPTY
    STORAGE_BRONZE_LINKS_STREAM = EMPTY
    STORAGE_BRONZE_MOVIES_STREAM = EMPTY
    STORAGE_BRONZE_RATINGS_STREAM = EMPTY
    STORAGE_BRONZE_TAGS_STREAM = EMPTY

    # STORAGE SILVER
    STORAGE_MOUNT_SILVER = EMPTY
    STORAGE_SILVER_GENOME_SCORES_STREAM = EMPTY
    STORAGE_SILVER_GENOME_TAGS_STREAM = EMPTY
    STORAGE_SILVER_LINKS_STREAM = EMPTY
    STORAGE_SILVER_MOVIES_STREAM = EMPTY
    STORAGE_SILVER_RATINGS_STREAM = EMPTY
    STORAGE_SILVER_TAGS_STREAM = EMPTY

    # STORAGE BRONZE CHECKPOINT
    STORAGE_TMP_BRONZE = EMPTY
    STORAGE_BRONZE_GENOME_SCORES_STREAM_CHECKPOINT = EMPTY
    STORAGE_BRONZE_GENOME_TAGS_STREAM_CHECKPOINT = EMPTY
    STORAGE_BRONZE_LINKS_STREAM_CHECKPOINT = EMPTY
    STORAGE_BRONZE_MOVIES_STREAM_CHECKPOINT = EMPTY
    STORAGE_BRONZE_RATINGS_STREAM_CHECKPOINT = EMPTY
    STORAGE_BRONZE_TAGS_STREAM_CHECKPOINT = EMPTY

    # STORAGE SILVER CHECKPOINT
    STORAGE_TMP_SILVER = EMPTY
    STORAGE_SILVER_GENOME_SCORES_STREAM_CHECKPOINT = EMPTY
    STORAGE_SILVER_GENOME_TAGS_STREAM_CHECKPOINT = EMPTY
    STORAGE_SILVER_LINKS_STREAM_CHECKPOINT = EMPTY
    STORAGE_SILVER_MOVIES_STREAM_CHECKPOINT = EMPTY
    STORAGE_SILVER_RATINGS_STREAM_CHECKPOINT = EMPTY
    STORAGE_SILVER_TAGS_STREAM_CHECKPOINT = EMPTY


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

        # STORAGE
        self.STORAGE_MOUNT_PATH = self.config.get("storage.mount.path")
        self.STORAGE_TMP_PATH = self.config.get("storage.tmp.path")

        # STORAGE EXTERNAL
        self.STORAGE_TMP_EXTERNAL = f"{self.STORAGE_TMP_PATH}{self.config.get('storage.container.name.external')}"
        self.STORAGE_EXTERNAL_GENOME_SCORES = f"{self.STORAGE_TMP_EXTERNAL}{self.config.get('storage.path.movilens.genome.scores')}"
        self.STORAGE_EXTERNAL_GENOME_TAGS = f"{self.STORAGE_TMP_EXTERNAL}{self.config.get('storage.path.movilens.genome.tags')}"
        self.STORAGE_EXTERNAL_LINKS = f"{self.STORAGE_TMP_EXTERNAL}{self.config.get('storage.path.movilens.links')}"
        self.STORAGE_EXTERNAL_MOVIES = f"{self.STORAGE_TMP_EXTERNAL}{self.config.get('storage.path.movilens.movies')}"
        self.STORAGE_EXTERNAL_RATINGS = f"{self.STORAGE_TMP_EXTERNAL}{self.config.get('storage.path.movilens.ratings')}"
        self.STORAGE_EXTERNAL_TAGS = f"{self.STORAGE_TMP_EXTERNAL}{self.config.get('storage.path.movilens.tags')}"

        # STORAGE TRANSIENT
        self.STORAGE_MOUNT_TRANSIENT = f"{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.external')}"
        self.STORAGE_TRANSIENT_GENOME_SCORES = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.movilens.genome.scores')}"
        self.STORAGE_TRANSIENT_GENOME_TAGS = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.movilens.genome.tags')}"
        self.STORAGE_TRANSIENT_LINKS = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.movilens.links')}"
        self.STORAGE_TRANSIENT_MOVIES = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.movilens.movies')}"
        self.STORAGE_TRANSIENT_RATINGS = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.movilens.ratings')}"
        self.STORAGE_TRANSIENT_TAGS = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.movilens.tags')}"

        # STORAGE BRONZE
        self.STORAGE_MOUNT_BRONZE = f"{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.bronze')}"
        self.STORAGE_BRONZE_GENOME_SCORES_STREAM = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.movilens.genome.scores')}/stream"
        self.STORAGE_BRONZE_GENOME_TAGS_STREAM = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.movilens.genome.tags')}/stream"
        self.STORAGE_BRONZE_LINKS_STREAM = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.movilens.links')}/stream"
        self.STORAGE_BRONZE_MOVIES_STREAM = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.movilens.movies')}/stream"
        self.STORAGE_BRONZE_RATINGS_STREAM = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.movilens.ratings')}/stream"
        self.STORAGE_BRONZE_TAGS_STREAM = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.movilens.tags')}/stream"

        # STORAGE SILVER
        self.STORAGE_MOUNT_SILVER = f"{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.silver')}"
        self.STORAGE_SILVER_GENOME_SCORES_STREAM = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.movilens.genome.scores')}/stream"
        self.STORAGE_SILVER_GENOME_TAGS_STREAM = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.movilens.genome.tags')}/stream"
        self.STORAGE_SILVER_LINKS_STREAM = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.movilens.links')}/stream"
        self.STORAGE_SILVER_MOVIES_STREAM = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.movilens.movies')}/stream"
        self.STORAGE_SILVER_RATINGS_STREAM = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.movilens.ratings')}/stream"
        self.STORAGE_SILVER_TAGS_STREAM = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.movilens.tags')}/stream"

        # STORAGE BRONZE CHECKPOINT
        self.STORAGE_TMP_BRONZE = f"{self.STORAGE_TMP_PATH}{self.config.get('storage.container.name.bronze')}"
        self.STORAGE_BRONZE_GENOME_SCORES_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_BRONZE}{self.config.get('storage.path.movilens.genome.scores')}/stream/_checkpoint"
        self.STORAGE_BRONZE_GENOME_TAGS_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_BRONZE}{self.config.get('storage.path.movilens.genome.tags')}/stream/_checkpoint"
        self.STORAGE_BRONZE_LINKS_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_BRONZE}{self.config.get('storage.path.movilens.links')}/stream/_checkpoint"
        self.STORAGE_BRONZE_MOVIES_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_BRONZE}{self.config.get('storage.path.movilens.movies')}/stream/_checkpoint"
        self.STORAGE_BRONZE_RATINGS_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_BRONZE}{self.config.get('storage.path.movilens.ratings')}/stream/_checkpoint"
        self.STORAGE_BRONZE_TAGS_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_BRONZE}{self.config.get('storage.path.movilens.tags')}/stream/_checkpoint"

        # STORAGE SILVER CHECKPOINT
        self.STORAGE_TMP_SILVER = f"{self.STORAGE_TMP_PATH}{self.config.get('storage.container.name.silver')}"
        self.STORAGE_SILVER_GENOME_SCORES_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_SILVER}{self.config.get('storage.path.movilens.genome.scores')}/stream/_checkpoint"
        self.STORAGE_SILVER_GENOME_TAGS_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_SILVER}{self.config.get('storage.path.movilens.genome.tags')}/stream/_checkpoint"
        self.STORAGE_SILVER_LINKS_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_SILVER}{self.config.get('storage.path.movilens.links')}/stream/_checkpoint"
        self.STORAGE_SILVER_MOVIES_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_SILVER}{self.config.get('storage.path.movilens.movies')}/stream/_checkpoint"
        self.STORAGE_SILVER_RATINGS_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_SILVER}{self.config.get('storage.path.movilens.ratings')}/stream/_checkpoint"
        self.STORAGE_SILVER_TAGS_STREAM_CHECKPOINT = f"{self.STORAGE_TMP_SILVER}{self.config.get('storage.path.movilens.tags')}/stream/_checkpoint"
