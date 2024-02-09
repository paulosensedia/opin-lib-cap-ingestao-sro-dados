from opin_lib_cap_ingestao_sro_dados.config.configuration import Configuration
from opin_lib_cap_ingestao_sro_dados.config.util.dbutils.dbutils import DBUtils
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from jproperties import Properties
import os


class DBUtilsDatabricks(DBUtils):

    def __init__(self, env: Environment, config: Configuration, dbutils):
        env = env
        self.dbutils = dbutils
        self.env = env
        self.config = config

        # self.dir_path = os.path.dirname(os.path.realpath(__file__))
        #
        # self.configuration = {}
        # conf = Properties()
        # conf_file = f"{self.dir_path}/configuration_{env}.properties"
        # with open(conf_file, "rb") as f:
        #     conf.load(f, "utf-8")
        #
        # for item in conf.items():
        #     key = item.__getitem__(0)
        #     value = item.__getitem__(1).data
        #     self.configuration.__setitem__(key, value)

    def fs_ls(self, path):
        return self.dbutils.fs.ls(path)

    def fs_rm(self, path, recursive: bool):
        return self.dbutils.fs.rm(path, recursive)

    def get_kvault(self, key: str):
        return self.dbutils.secrets.get(
            scope=f"azu-bsengdadossecretscope-{str(self.env.env_current.value).lower()}-opin-001",
            key=self.config.get(key))
