from abc import ABC, abstractmethod

from opin_lib_cap_ingestao_sro_dados.config.configuration import Configuration
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment


class DBUtilsFactory(ABC):

    @abstractmethod
    def create_instance(self, env: Environment, conf: Configuration, dbutils):
        pass

    def get_instance(self, env: Environment, conf: Configuration, dbutils):
        return self.create_instance(env, conf, dbutils)
