from abc import ABC, abstractmethod

from opin_lib_testes_conexoes.config.util.environment import Environment


class DBUtils(ABC):
    """
    Classe abstrata que o intuito de fornecer uma abstração às funcionalidades
    dbutils e secrets do Databricks. Logo, permite a emulação desses em
    ambiente local.
    """

    _env = None

    def __init__(self, env: Environment):
        self._env = env

    @abstractmethod
    def fs_ls(self, path):
        pass

    @abstractmethod
    def get_kvault(self, key: str):
        pass

    @abstractmethod
    def fs_rm(self, path, recursive: bool):
        pass

    @abstractmethod
    def get_kvault_cosmosdb(self, key):
        pass

    @abstractmethod
    def get_kvault_engdados(self, key):
        pass


    @abstractmethod
    def get_kvault_kafka_config(self, username_key: str, password_key: str):
        pass

    @abstractmethod
    def widgets_get(self, file_name: str):
        pass

    @abstractmethod
    def notebook_exit(self, variable):
        pass