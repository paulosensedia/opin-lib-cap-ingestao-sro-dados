import csv
import os
from unittest import mock
from unittest.mock import MagicMock, patch
import pytest
from opin_lib_cap_ingestao_sro_dados.ingestion import openhub_full as ophf


@pytest.mark.usefixtures("spark_session")
def test_get_tables_name_full(spark_session):
    tables_path = "../tempfiles/testFileOpenhub.csv"

    with mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.is_empty") as mock_is_empty:
        mock_is_empty.return_value = False
        tables = ophf.get_tables_name_full(None, spark_session, tables_path)
        tables.show(5, False)

    assert tables.count() >= 1


@pytest.mark.usefixtures("spark_session")
def test_ingestion_openhub_full(spark_session):
    dbutils_mock = MagicMock()

    input_path = "input_path"
    output_path = "output_path"
    tables_path = "tables_path"
    ramos_path = "ramos_path"
    context = MagicMock()

    COLUMNS = [
        "CRAMO",
        "RRAMO",
        "RGRP_RAMO"
    ]

    schema_tables = [
        "base",
        "table",
        "chaves",
        "path_source",
        "path_sink"
    ]

    schema_full = ['OHREQUID', 'DATAPAKID', 'RECORD', '/BIC/ZCHREQUID', '/BIC/ZCHTPEVOP', 'COMP_CODE',
                          '/BIC/ZCHSUCURS', '/BIC/ZCHRAMO', '/BIC/ZCHAPOLIC', '/BIC/ZCHENDOSS', '/BIC/ZCHUNICO',
                          '/BIC/ZCHPROPOS', '/BIC/ZCHSININT', '/BIC/ZDTMOVIME', '/BIC/ZKFVLRCOS', '/BIC/ZCHCSAQRD',
                          '/BIC/ZKFCUSAP', 'CURRENCY', '/BIC/ZCHCOTACA', '/BIC/ZCHTPBLOQ', '/BIC/ZCHDESBLO', 'ZCHDESAD',
                          '/BIC/ZKFPREINI', '/BIC/ZKFVLRDEP', '/BIC/ZKFPREAJU', '/BIC/ZKFLIMCRE', '/BIC/ZCHTPLIMI',
                          'ZCHDESLIM', '/BIC/ZCHTPREGI', '/BIC/ZCHCODLID', '/BIC/ZCHAPLDCS', '/BIC/ZKFIMPSEG',
                          '/BIC/ZCHMOEDA', '/BIC/ZKFIOF', '/BIC/ZKFADFRA', '/BIC/ZKFCTAQOP', '/BIC/ZKFPREEM',
                          'FC_BETRW',
                          'TB_TCUR', '/BIC/ZKFPRECOS', '/BIC/ZCHUF', '/BIC/ZCHCOLET', '/BIC/ZCHINDCOS', '/BIC/ZDTFIMVG',
                          '/BIC/ZDTINIVG', '/BIC/ZDTEMISS', '/BIC/ZCHAPOSRO', '/BIC/ZCHORISRO', '/BIC/ZCHNRSEQ',
                          '/BIC/ZCHPROCSU', '/BIC/ZCHSTCONT', '/BIC/ZCHORIGEM', '/BIC/ZCHCOBASI', '/BIC/ZCHNSUSEP',
                          '/BIC/ZCHNDOC', '/BIC/ZCHPROASC', '/BIC/ZCHENDASC', '/BIC/ZCHNCERT', 'path_source',
                          'path_sink', 'base', 'table']

    df = spark_session.createDataFrame(
        [("2", "PECULIO", "Previdencia")],
        COLUMNS)

    df_tabelas = spark_session.createDataFrame(
        [("premio", "ZOHOPIN02",
          "ZCHTPEVOP, COMP_CODE, ZCHSUCURS, ZCHRAMO, ZCHAPOLIC, ZCHENDOSS, ZCHUNICO, ZCHPROPOS, ZCHSININT, ZDTMOVIME, ZCHNDOC",
          "ZOHOPIN02", "ZOHOPIN02")],
        schema_tables)

    df_delta_table = spark_session.createDataFrame(
        [(445336, 4, 80, "0000445336", "003", "5312", "0939", "0114", "00000000000000028414",
         "00000000000000000000", "927000028191528", "P028191528", "", "20210524", 0.00, 0.00, 0.00, "BRL",
         "", "00", "", "", 0.00, 0.00, 0.00, 328500.00, "00", "", "RA", "", "", 328500.00, "986", 33.12, 0.00, 84.37,
         421.88, 455.00, "BRL", 0.00, "SP", "1", "1", "20220520", "20210520", "20210524",
         "000000000000000000000000000000000000000093992724400284140001", "H", "001222961276", "15414.005043/2005-18",
         "", "", "01", "00000000000000000000053122021093901140028414000000", "", "", "", "", "teste", "teste",
          "sinistro", "ZOHOPIN02")],
        schema_full)

    read_delta_file_mock = MagicMock(return_value=df)
    get_tables_name_full_mock = MagicMock(return_value=df_tabelas)
    read_csv_file_mock = MagicMock(return_value=df_delta_table)
    write_delta_file_mock = MagicMock()
    delete_mock = MagicMock()
    valida_volumetria_mock = MagicMock()
    valida_obrigatoriedade_mock = MagicMock()
    valida_tipagem_mock = MagicMock()

    with patch("opin_lib_cap_ingestao_sro_dados.storage_functions.read_delta_file", read_delta_file_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.ingestion.openhub_full.get_tables_name_full",
                  get_tables_name_full_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.read_csv_file", read_csv_file_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.write_delta_file", write_delta_file_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.delete", delete_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.common_functions.valida_volumetria", valida_volumetria_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.common_functions.valida_obrigatoriedade",
                  valida_obrigatoriedade_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.common_functions.valida_tipagem", valida_tipagem_mock):
        ophf.ingestion(dbutils_mock, spark_session, context, ramos_path, tables_path, input_path, output_path)

