from pyspark.sql import SparkSession

from opin_lib_cap_ingestao_sro_dados.config.util.dbutils.dbutils_helper import DBUtilsHelper
from opin_lib_cap_ingestao_sro_dados.config.util.logger import Logger
from opin_lib_cap_ingestao_sro_dados.config.configuration import Configuration
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_cap_ingestao_sro_dados.config.util.spark.app_spark_context_helper import SparkContextHelper

class Context:
    """
    O objetivo dessa classe Context é iniciar o contexto da aplicação de acordo
    com o ambiente em que é executado. Logo, torna-se um ponto comum para
    acesso a recursos externos e específicos de cada ambiente.
    """

    _LOG = Logger(path=__name__, name=__qualname__)
    _LOG.info("Initializing application context.")

    EMPTY = ''

    STORAGE_MOUNT_PATH = '/mnt/'

    # STORAGE PROPERTIES - TRANSIENT
    STORAGE_MOUNT_TRANSIENT = EMPTY
    STORAGE_TRANSIENT_INGESTAO_RAMO_SUSEP = EMPTY
    STORAGE_TRANSIENT_INGESTAO_TABLES_FULL = EMPTY
    STORAGE_TRANSIENT_INGESTAO_TABLES_DELTA = EMPTY
    STORAGE_TRANSIENT_INGESTAO_SRO = EMPTY
    STORAGE_TRANSIENT_INGESTAO_SRO_FULL = EMPTY
    STORAGE_TRANSIENT_INGESTAO_DELTA= EMPTY

    # STORAGE PROPERTIES - BRONZE
    STORAGE_MOUNT_BRONZE = EMPTY
    STORAGE_RAW_INGESTAO_RAMO_SUSEP = EMPTY
    STORAGE_RAW_INGESTAO_TABLES_FULL = EMPTY
    STORAGE_RAW_INGESTAO_TABLES_DELTA = EMPTY
    STORAGE_RAW_INGESTAO_SRO = EMPTY

    # STORAGE PROPERTIES - SILVER
    STORAGE_MOUNT_SILVER = EMPTY
    STORAGE_TRUSTED_INGESTAO_RAMO_SUSEP = EMPTY
    STORAGE_TRUSTED_INGESTAO_TABLES_FULL = EMPTY
    STORAGE_TRUSTED_INGESTAO_TABLES_DELTA = EMPTY
    STORAGE_TRUSTED_INGESTAO_SRO = EMPTY

    # STORAGE PROPERTIES - GOLD
    STORAGE_MOUNT_GOLD = EMPTY

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

        # STORAGE
        self.STORAGE_MOUNT_PATH = self.config.get("storage.mount.path")

        # STORAGE TRANSIENT
        self.STORAGE_MOUNT_TRANSIENT = f"{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.transient')}"
        self.STORAGE_TRANSIENT_INGESTAO_RAMO_SUSEP = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.ingestao.ramo_susep')}"
        self.STORAGE_TRANSIENT_INGESTAO_TABLES_FULL = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.ingestao.tables_full')}"
        self.STORAGE_TRANSIENT_INGESTAO_TABLES_DELTA = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.ingestao.tables_delta')}"
        self.STORAGE_TRANSIENT_INGESTAO_SRO = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.ingestao.sro')}"
        self.STORAGE_TRANSIENT_INGESTAO_SRO_FULL = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.ingestao.sro_full')}"
        self.STORAGE_TRANSIENT_INGESTAO_SRO_DELTA = f"{self.STORAGE_MOUNT_TRANSIENT}{self.config.get('storage.path.ingestao.sro_delta')}"

        # STORAGE BRONZE
        self.STORAGE_MOUNT_BRONZE = f"{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.bronze')}"
        self.STORAGE_RAW_INGESTAO_RAMO_SUSEP = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.ingestao.ramo_susep')}"
        self.STORAGE_RAW_INGESTAO_TABLES_FULL = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.ingestao.tables_full')}"
        self.STORAGE_RAW_INGESTAO_TABLES_DELTA = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.ingestao.tables_delta')}"
        self.STORAGE_RAW_INGESTAO_SRO = f"{self.STORAGE_MOUNT_BRONZE}{self.config.get('storage.path.ingestao.sro')}"

        # STORAGE SILVER
        self.STORAGE_MOUNT_SILVER = f"{self.STORAGE_MOUNT_PATH}{self.config.get('storage.container.name.silver')}"
        self.STORAGE_TRUSTED_INGESTAO_RAMO_SUSEP = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.ingestao.ramo_susep')}"
        self.STORAGE_TRUSTED_INGESTAO_TABLES_FULL = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.ingestao.tables_full')}"
        self.STORAGE_TRUSTED_INGESTAO_TABLES_DELTA = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.ingestao.tables_delta')}"
        self.STORAGE_TRUSTED_INGESTAO_SRO = f"{self.STORAGE_MOUNT_SILVER}{self.config.get('storage.path.ingestao.sro')}"
