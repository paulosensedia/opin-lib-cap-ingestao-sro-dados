from opin_lib_chassis_dados.config.configuration import Configuration
from opin_lib_chassis_dados.config.util.dbutils.dbutils import DBUtils
from opin_lib_chassis_dados.config.util.environment import Environment


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
