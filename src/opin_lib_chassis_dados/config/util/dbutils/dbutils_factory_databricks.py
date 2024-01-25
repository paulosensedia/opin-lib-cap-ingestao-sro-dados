from opin_lib_chassis_dados.config.configuration import Configuration
from opin_lib_chassis_dados.config.util.dbutils.dbutils_databricks import DBUtilsDatabricks
from opin_lib_chassis_dados.config.util.dbutils.dbutils_factory import DBUtilsFactory
from opin_lib_chassis_dados.config.util.environment import Environment


class DBUtilsFactoryDatabricks(DBUtilsFactory):
    """
    Classe responsável por retornar a instância de Databricks.dbutils.
    """

    def create_instance(self, env: Environment, config: Configuration, dbutils):
        return DBUtilsDatabricks(env, config, dbutils)
