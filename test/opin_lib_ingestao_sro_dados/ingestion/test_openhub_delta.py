import csv
import os
from unittest import mock
from unittest.mock import MagicMock, patch
import pytest
from opin_lib_cap_ingestao_sro_dados.ingestion import openhub_delta as oph


def test_create_file(spark_session):
    diretorio = "../tempfiles"

    # Verifica se o diretório não existe antes de criá-lo
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)

    with open('../tempfiles/testFileOpenhub.csv', 'w', newline='') as csvfile:
        fieldnames = ['OHREQUID', 'DATAPAKID', 'RECORD', '/BIC/ZCHREQUID', '/BIC/ZCHTPEVOP', 'COMP_CODE',
                      '/BIC/ZCHSUCURS', '/BIC/ZCHRAMO', '/BIC/ZCHAPOLIC', '/BIC/ZCHENDOSS', '/BIC/ZCHUNICO',
                      '/BIC/ZCHPROPOS', '/BIC/ZCHSININT', '/BIC/ZDTMOVIME', '/BIC/ZKFVLRCOS', '/BIC/ZCHCSAQRD',
                      '/BIC/ZKFCUSAP', 'CURRENCY', '/BIC/ZCHCOTACA', '/BIC/ZCHTPBLOQ', '/BIC/ZCHDESBLO', 'ZCHDESAD',
                      '/BIC/ZKFPREINI', '/BIC/ZKFVLRDEP', '/BIC/ZKFPREAJU', '/BIC/ZKFLIMCRE', '/BIC/ZCHTPLIMI',
                      'ZCHDESLIM', '/BIC/ZCHTPREGI', '/BIC/ZCHCODLID', '/BIC/ZCHAPLDCS', '/BIC/ZKFIMPSEG',
                      '/BIC/ZCHMOEDA', '/BIC/ZKFIOF', '/BIC/ZKFADFRA', '/BIC/ZKFCTAQOP', '/BIC/ZKFPREEM', 'FC_BETRW',
                      'TB_TCUR', '/BIC/ZKFPRECOS', '/BIC/ZCHUF', '/BIC/ZCHCOLET', '/BIC/ZCHINDCOS', '/BIC/ZDTFIMVG',
                      '/BIC/ZDTINIVG', '/BIC/ZDTEMISS', '/BIC/ZCHAPOSRO', '/BIC/ZCHORISRO', '/BIC/ZCHNRSEQ',
                      '/BIC/ZCHPROCSU', '/BIC/ZCHSTCONT', '/BIC/ZCHORIGEM', '/BIC/ZCHCOBASI', '/BIC/ZCHNSUSEP',
                      '/BIC/ZCHNDOC', '/BIC/ZCHPROASC', '/BIC/ZCHENDASC', '/BIC/ZCHNCERT', 'path_source', 'base',
                      'table', 'ZCHREQUID']

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='|')
        writer.writeheader()
        writer.writerow(
            {'OHREQUID': 445336, 'DATAPAKID': 4, 'RECORD': 80, '/BIC/ZCHREQUID': "0000445336", '/BIC/ZCHTPEVOP': "003",
             'COMP_CODE': "5312", '/BIC/ZCHSUCURS': "0939", '/BIC/ZCHRAMO': "0114",
             '/BIC/ZCHAPOLIC': "00000000000000028414", '/BIC/ZCHENDOSS': "00000000000000000000",
             '/BIC/ZCHUNICO': "927000028191528", '/BIC/ZCHPROPOS': "P028191528", '/BIC/ZCHSININT': "",
             '/BIC/ZDTMOVIME': "20210524", '/BIC/ZKFVLRCOS': 0.00, '/BIC/ZCHCSAQRD': 0.00, '/BIC/ZKFCUSAP': 0.00,
             'CURRENCY': "BRL", '/BIC/ZCHCOTACA': "", '/BIC/ZCHTPBLOQ': "00", '/BIC/ZCHDESBLO': "", 'ZCHDESAD': "",
             '/BIC/ZKFPREINI': 0.00, '/BIC/ZKFVLRDEP': 0.00, '/BIC/ZKFPREAJU': 0.00, '/BIC/ZKFLIMCRE': 328500.00,
             '/BIC/ZCHTPLIMI': "00", 'ZCHDESLIM': "", '/BIC/ZCHTPREGI': "RA", '/BIC/ZCHCODLID': "",
             '/BIC/ZCHAPLDCS': "", '/BIC/ZKFIMPSEG': 328500.00, '/BIC/ZCHMOEDA': "986", '/BIC/ZKFIOF': 33.12,
             '/BIC/ZKFADFRA': 0.00, '/BIC/ZKFCTAQOP': 84.37, '/BIC/ZKFPREEM': 421.88, 'FC_BETRW': 455.00,
             'TB_TCUR': "BRL", '/BIC/ZKFPRECOS': 0.00, '/BIC/ZCHUF': "SP", '/BIC/ZCHCOLET': "1", '/BIC/ZCHINDCOS': "1",
             '/BIC/ZDTFIMVG': "20220520", '/BIC/ZDTINIVG': "20210520", '/BIC/ZDTEMISS': "20210524",
             '/BIC/ZCHAPOSRO': "000000000000000000000000000000000000000093992724400284140001", '/BIC/ZCHORISRO': "H",
             '/BIC/ZCHNRSEQ': "001222961276", '/BIC/ZCHPROCSU': "15414.005043/2005-18", '/BIC/ZCHSTCONT': "",
             '/BIC/ZCHORIGEM': "", '/BIC/ZCHCOBASI': "01",
             '/BIC/ZCHNSUSEP': "00000000000000000000053122021093901140028414000000", '/BIC/ZCHNDOC': "",
             '/BIC/ZCHPROASC': "", '/BIC/ZCHENDASC': "", '/BIC/ZCHNCERT': "", 'path_source': "teste", 'base': 'RRRR',
             'table': 'TABELA_TESTE', 'ZCHREQUID': 445336})

    spark_session.stop()


@pytest.mark.usefixtures("spark_session")
def test_get_tables_name_delta(spark_session):
    tables_path = "../tempfiles/testFileOpenhub.csv"

    with mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.is_empty") as mock_is_empty:
        mock_is_empty.return_value = False
        tables = oph.get_tables_name_delta(None, spark_session, tables_path)
        tables.show(5, False)

    assert tables.count() >= 1


@pytest.mark.usefixtures("spark_session")
def test_max_delta(spark_session):
    tables_path = "../tempfiles/testFileOpenhub.csv"

    with mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.is_empty") as mock_is_empty:
        mock_is_empty.return_value = False
        tables = oph.get_tables_name_delta(None, spark_session, tables_path)
        tables.show(5, False)

    chaves_list = ['OHREQUID', 'DATAPAKID']
    df = oph.max_delta(tables, chaves_list)
    df.show(5, False)

    assert df.count() >= 1


@pytest.mark.usefixtures("spark_session")
def test_ingestion_openhub_delta(spark_session):
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

    schema_delta_table = ['OHREQUID', 'DATAPAKID', 'RECORD', '/BIC/ZCHREQUID', '/BIC/ZCHTPEVOP', 'COMP_CODE',
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
        schema_delta_table)

    read_delta_file_mock = MagicMock(return_value=df)
    get_tables_name_delta_mock = MagicMock(return_value=df_tabelas)
    read_csv_file_mock = MagicMock(return_value=df_delta_table)
    write_delta_file_mock = MagicMock()
    delete_mock = MagicMock()
    valida_volumetria_mock = MagicMock()
    valida_obrigatoriedade_mock = MagicMock()
    valida_tipagem_mock = MagicMock()

    with patch("opin_lib_cap_ingestao_sro_dados.storage_functions.read_delta_file", read_delta_file_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.ingestion.openhub_delta.get_tables_name_delta",
                  get_tables_name_delta_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.read_csv_file", read_csv_file_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.write_delta_file", write_delta_file_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.delete", delete_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.common_functions.valida_volumetria", valida_volumetria_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.common_functions.valida_obrigatoriedade",
                  valida_obrigatoriedade_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.common_functions.valida_tipagem", valida_tipagem_mock):
        oph.ingestion(dbutils_mock, spark_session, context, ramos_path, tables_path, input_path, output_path)

