from enum import Enum, unique

from opin_lib_chassis_dados.config.util.environment_exception import EnvironmentException


@unique
class EnvironmentEnum(Enum):

    LOCAL_WIN = "LOCAL_WIN"
    LOCAL_LINUX = "LOCAL_LINUX"
    TU = "TU"
    TI = "TI"
    TH = "TH"
    PR = "PR"

    @classmethod
    def get(cls, env: str):
        try:
            return EnvironmentEnum[env]
        except KeyError:
            msg = "There is no EnvironmentEnum defined for: {0}".format(env)
            raise EnvironmentException(msg)
