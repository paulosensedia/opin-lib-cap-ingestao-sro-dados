from opin_lib_cap_ingestao_sro_dados.config.configuration import Configuration
from opin_lib_cap_ingestao_sro_dados.config.util.dbutils.dbutils import DBUtils
from opin_lib_cap_ingestao_sro_dados.config.util.dbutils.dbutils_factory import DBUtilsFactory
from opin_lib_cap_ingestao_sro_dados.config.util.dbutils.dbutils_local import DBUtilsLocal
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment


class DBUtilsFactoryLocal(DBUtilsFactory):
    """
    Classe responsável por retornar a instância emudala de Databricks.dbutils.
    """

    def create_instance(self, env: Environment, config: Configuration, dbutils) -> DBUtils:
        return DBUtilsLocal(env, config)


