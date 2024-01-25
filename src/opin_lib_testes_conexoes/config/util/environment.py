import os

from opin_lib_testes_conexoes.config.util.environment_enum import EnvironmentEnum


class Environment:
    """
    Essa classe é responsável por identificar o ambiente em que a aplicação
    está sendo executada.
    """

    def __init__(self):
        self._env_default = EnvironmentEnum.PR
        self._env_current = EnvironmentEnum.get(self.get_env())

    @property
    def env_default(self) -> EnvironmentEnum:
        return self._env_default

    @property
    def env_current(self) -> EnvironmentEnum:
        return self._env_current

    def get_env(self):
        """
        Esse método têm por finalidade identificar qual é o ambiente em que a
        aplicação está sendo executada.

        Nos clusters Databricks foi definido uma variável de ambiente denomiada
        AMBIENTE, podendo assumir um dos seguintes valores: TI, TU, TH e PR.
        Em ambiente local (por exemplo Windows e Linux) a identificação é
        feito através da biblioteca OS do Python. Com isso, é possível definir
        os seguintes valores de identificação: LOCAL_WIN e LOCAL_LINUX.
        """
        env = os.getenv("AMBIENTE")
        if env is not None:
            return env.upper()

        import platform
        so = platform.system()
        if so == "Linux":
            return EnvironmentEnum.LOCAL_LINUX.value
        elif so == "Windows":
            return EnvironmentEnum.LOCAL_WIN.value
        else:
            return None
