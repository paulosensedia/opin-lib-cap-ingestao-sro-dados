from jproperties import Properties
import os

from opin_lib_chassis_dados.config.util.environment import Environment
from opin_lib_chassis_dados.config.util.logger import Logger
from opin_lib_chassis_dados.config.util.environment_enum import EnvironmentEnum


class Configuration:
    """
    Esta classe possui apenas uma responsabilidade: carregar propriedades
    definidas em arquivos .properties.

    Arquivos de propriedades têm a finalidade de armazenar valores pertinentes
    ao ambiente, por exemplo: a nome de uma secret no Key Vault; diretórios no
    Data Lake; URL de bases de dados. Há um arquivo de propriedades para cada
    ambiente no mesmo diretório desta classe. Os arquivos de propriedades
    possuem o seguinte sufixo: *env.properties.

    O comportamento padrão de leitura dos arquivos por essa classe é feito em
    duas partes: Carregar arquivo de propriedades de produção (como padrão);
    Carregar arquivo de propriedades do ambiente atual (corrente). As
    propriedades definidas no arquivo atual sobrescreverá àquelas padrão.
    """
    _LOG = Logger(path=__name__, name=__qualname__)

    def __init__(self, env: Environment):
        self._configuration = {}

        default_file_path = self.get_file_path(env.env_default)
        default_file = self.read_file_properties(default_file_path)
        self.load(default_file, self._configuration)

        if env != env.env_current:
            current_file_path = self.get_file_path(env.env_current)
            current_file = self.read_file_properties(current_file_path)
            self.load(current_file, self._configuration)

    def read_file_properties(self, file_path) -> Properties:
        self._LOG.info("Read properties file: {0}.".format(file_path))
        prop = Properties()
        with open(file_path, "rb") as content:
            prop.load(content, "utf-8")
        return prop

    def get_file_path(self, env: EnvironmentEnum) -> str:
        basepath = os.path.dirname(os.path.realpath(__file__))
        file_path = "{0}/{1}_env.properties".format(basepath, env.value.lower())
        return file_path

    def load(self, file, configuration: Properties):
        for item in file.items():
            key = item.__getitem__(0)
            value = item.__getitem__(1).data
            configuration.__setitem__(key, value)
        self._LOG.info("Properties loaded.")

    def get(self, key):
        return self._configuration.get(key)
