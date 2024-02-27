import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession

from opin_lib_cap_ingestao_sro_dados.config.context import Context
from opin_lib_cap_ingestao_sro_dados.config.configuration import Configuration
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_cap_ingestao_sro_dados.config.util.environment_enum import EnvironmentEnum
from opin_lib_cap_ingestao_sro_dados.config.util.dbutils.dbutils_helper import DBUtilsHelper


@pytest.fixture
def spark_session():
    return(SparkSession.builder
           .enableHiveSupport()
           .getOrCreate())


@pytest.fixture
def dbutils_helper():
    env = Environment()
    return DBUtilsHelper().get_instance(env, Configuration(env), None)


@pytest.fixture
def dbutils_mock():
    return MagicMock()

