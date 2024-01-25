import os

from opin_lib_testes_conexoes.config.configuration import Configuration
from opin_lib_testes_conexoes.config.util.dbutils.dbutils import DBUtils
from opin_lib_testes_conexoes.config.util.environment import Environment


class DBUtilsLocal(DBUtils):
    """
    Classe responsável por emular no ambiente local as ferramentas:

    Databricks.dbutils
        utiliza-se a biblioteca OS do Python.

    Azure Key Vault
        utiliza-se o arquivo de propriedades.
    """

    def __init__(self, env: Environment, config: Configuration):
        self._env = env
        self._dbutils = os
        self._config = config

    def fs_ls(self, path):
        return self._dbutils.listdir(path)

    def fs_rm(self, path, recursive: bool):
        return self._dbutils.remove(path)

    def get_kvault(self, key: str):
        """
        Este método simula o acesso Cloud ao Key Vault para obtenção de valores
        de segredos. Com isso, os valores são obtidos diretamente dos arquivos
        de propriedades.

        Args:
            key (str): nome do segredo.

        Returns:
            O valor do segredo.
        """
        return self._config.get(key)
