from opin_lib_testes_conexoes.config.configuration import Configuration
from opin_lib_testes_conexoes.config.util.dbutils.dbutils import DBUtils
from opin_lib_testes_conexoes.config.util.dbutils.dbutils_factory_databricks import DBUtilsFactoryDatabricks
from opin_lib_testes_conexoes.config.util.dbutils.dbutils_factory_local import DBUtilsFactoryLocal
from opin_lib_testes_conexoes.config.util.environment import Environment
from opin_lib_testes_conexoes.config.util.environment_enum import EnvironmentEnum

class DBUtilsHelper:
    @staticmethod
    def get_instance(env: Environment, config: Configuration, dbutils) -> DBUtils:
        if env.env_current in (EnvironmentEnum.LOCAL_WIN, EnvironmentEnum.LOCAL_LINUX):
            return DBUtilsFactoryLocal().get_instance(env, config, dbutils)
        return DBUtilsFactoryDatabricks().get_instance(env, config, dbutils)