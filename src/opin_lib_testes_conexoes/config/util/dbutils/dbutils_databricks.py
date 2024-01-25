from opin_lib_testes_conexoes.config.configuration import Configuration
from opin_lib_testes_conexoes.config.util.dbutils.dbutils import DBUtils
from opin_lib_testes_conexoes.config.util.environment import Environment


class DBUtilsDatabricks(DBUtils):

    def __init__(self, env: Environment, config: Configuration, dbutils):
        self._env = env
        self._dbutils = dbutils
        self._config = config
        self._secret_scope_name = self._get_secret_scope_name(env)

    def fs_ls(self, path):
        return self._dbutils.fs.ls(path)

    def fs_rm(self, path, recursive: bool):
        return self._dbutils.fs.rm(path, recursive)

    def get_kvault(self, key: str) -> str:
        """
        Método responsável por obter valor de um segredo no Key Vault.

        Args:
            key (str): nome do segredo.

        Returns:
            O valor do segredo.
        """

        scope = f"{self._secret_scope_name}"
        key = self._config.get(key)

        return self._dbutils.secrets.get(scope=scope, key=key)

    @staticmethod
    def _get_secret_scope_name(env: Environment):
        env_current = env.env_current.value.lower()
        prefix_secret_scope_name = f"azu-bsdtbsecretscope-{env_current}"
        if "ti" == env_current:
            return f"{prefix_secret_scope_name}-opin-002"
        return f"{prefix_secret_scope_name}-opin-001"
    
    def get_kvault_cosmosdb(self, key):
        key = self._config.get(key)
        return self._dbutils.secrets.get(scope=f"azu-bscosmossecretscope-{self._env.env_current.value.lower()}-opin-001", key=key)

    def get_kvault_engdados(self, key):
        key = self._config.get(key)
        return self._dbutils.secrets.get(scope=f"azu-bsengdadossecretscope-{self._env.env_current.value.lower()}-opin-001", key=key)
    
    def get_kvault_kafka_config(self, username_key: str, password_key: str):
        username_key = self._config.get(username_key)
        password_key = self._config.get(password_key)

        username = self._dbutils.secrets.get(
            scope=f"azu-bskafkasecretscope-{self._env.env_current.value.lower()}-opin-001", key=username_key)
        password = self._dbutils.secrets.get(
            scope=f"azu-bskafkasecretscope-{self._env.env_current.value.lower()}-opin-001", key=password_key)

        config = f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";"""

        return config

    def notebook_exit(self, variable):
        self._dbutils.notebook.exit(variable)

    def widgets_get(self, file_name: str):
        return self._dbutils.widgets.get(file_name)
