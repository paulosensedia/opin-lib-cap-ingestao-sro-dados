from opin_lib_cap_ingestao_sro_dados.vacuum import vacuum_functions as vacf


def test_vacuum_delta(spark_session):
    vacum_log_schema = vacf.vacuum_log_schema()


# import csv
# import os
# from unittest.mock import MagicMock, patch
#
# import pytest
# from opin_lib_cap_ingestao_sro_dados.config.context import Context
# from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
# def test_vacuum_delta(spark_session):
#     diretorio = "./testfileVacuum"
#     VALID_FILE_NAME = 'testFile.csv'
#     valid_path = './testfileVacuum'
#
#     vacum_log_schema = vacf.vacuum_log_schema()
#
#     env = Environment()
#     context = Context(None, env, None)
#     dbutils = context.dbutils
#
#     # Mock para get_files
#     is_empty_mock = MagicMock(return_value=False)
#     get_table_size_mock = MagicMock(return_value=100)
#
#     with patch("opin_lib_cap_ingestao_sro_dados.storage_functions.is_empty", is_empty_mock), \
#             patch("opin_lib_cap_ingestao_sro_dados.vacuum.vacuum_functions.get_table_size", get_table_size_mock):
#         vacf.vacuum_delta(dbutils, spark_session, 'TABELA', valid_path)
