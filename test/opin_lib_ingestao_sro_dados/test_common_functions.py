import pytest
from unittest.mock import patch, MagicMock
from opin_lib_cap_ingestao_sro_dados.common_functions import valida_obrigatoriedade, valida_volumetria, valida_tipagem
from opin_lib_cap_ingestao_sro_dados.common_functions_enum import CommonFunctionsEnum


@pytest.mark.usefixtures("spark_session")
def test_common_functions_enum(spark_session):
    assert CommonFunctionsEnum.CAMPOS_OBRIGATORIOS_PREENCHIDOS_INCORRETAMENTE.value == 'camposObrigatoriosPreenchidosIncorretamente'
    assert CommonFunctionsEnum.TAMANHO_CAMPO_INVALIDO.value == 'tamanhoCampoInvalido'
    assert CommonFunctionsEnum.OPCOES_PREENCHIMENTO_INVALIDO.value == 'opcoesPreenchimentoInvalido'


@pytest.mark.usefixtures("spark_session")
def test_valida_volumetria(spark_session):
    dbutils_mock = MagicMock()

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
                          'path_sink', 'base', 'table', 'DT_INGESTAO_BRONZE']

    df_delta_table = spark_session.createDataFrame(
        [(445336, 4, 80, "0000445336", "003", "5312", "0939", "0114", "00000000000000028414",
          "00000000000000000000", "927000028191528", "P028191528", "", "20210524", 0.00, 0.00, 0.00, "BRL",
          "", "00", "", "", 0.00, 0.00, 0.00, 328500.00, "00", "", "RA", "", "", 328500.00, "986", 33.12, 0.00, 84.37,
          421.88, 455.00, "BRL", 0.00, "SP", "1", "1", "20220520", "20210520", "20210524",
          "000000000000000000000000000000000000000093992724400284140001", "H", "001222961276", "15414.005043/2005-18",
          "", "", "01", "00000000000000000000053122021093901140028414000000", "", "", "", "", "teste", "teste",
          "sinistro", "ZOHOPIN02", '2023-09-15')],
        schema_delta_table)

    table = 'table'
    path_bronze = './tempfiles'
    path_validacao = 'path_validacao'

    read_delta_file_mock = MagicMock(return_value=df_delta_table)
    write_delta_file_mock = MagicMock()
    delta_table = MagicMock()

    with patch("opin_lib_cap_ingestao_sro_dados.storage_functions.read_delta_file", read_delta_file_mock), \
            patch("delta.tables.DeltaTable.forPath", delta_table), \
            patch("pyspark.sql.session.SparkSession.sql", delta_table), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.write_delta_file", write_delta_file_mock):
        valida_volumetria(dbutils_mock, spark_session, table, path_bronze, path_validacao)


@pytest.mark.usefixtures("spark_session")
def test_valida_obrigatoriedade(spark_session):
    dbutils_mock = MagicMock()

    schema_delta_table = ['OHREQUID', 'DT_INGESTAO_BRONZE', 'obrigatorio']

    df_delta_table = spark_session.createDataFrame(
        [('OHREQUID', '2023-09-15', 'true')],
        schema_delta_table)

    path_bronze = './tempfiles'
    path_validacao = 'path_validacao'
    chaves = ['OHREQUID']
    path_parametros = 'parametros'

    read_csv_file_mock = MagicMock(return_value=df_delta_table)
    write_delta_file_mock = MagicMock()
    spark_read_format_mock = MagicMock()
    delta_table = MagicMock(return_value=df_delta_table)

    with patch("opin_lib_cap_ingestao_sro_dados.storage_functions.read_csv_file", read_csv_file_mock), \
            patch("pyspark.sql.readwriter.DataFrameReader.format", spark_read_format_mock), \
            patch("pyspark.sql.session.SparkSession.sql", delta_table), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.write_delta_file", write_delta_file_mock):
        valida_obrigatoriedade(dbutils_mock, spark_session, chaves, path_parametros, path_bronze, path_validacao)


@pytest.mark.usefixtures("spark_session")
def test_valida_tipagem(spark_session):
    dbutils_mock = MagicMock()

    schema_delta_table = ['campo', 'tipo', 'DT_INGESTAO_BRONZE', 'obrigatorio']

    df_delta_table = spark_session.createDataFrame(
        [('campo', 'tipo', '2023-09-15', 'true')],
        schema_delta_table)

    path_bronze = './tempfiles'
    path_validacao = 'path_validacao'
    table = 'TABLE'
    path_parametros = 'parametros'

    read_csv_file_mock = MagicMock(return_value=df_delta_table)
    write_delta_file_mock = MagicMock()
    spark_read_format_mock = MagicMock()
    delta_table = MagicMock(return_value=df_delta_table)

    with patch("opin_lib_cap_ingestao_sro_dados.storage_functions.read_csv_file", read_csv_file_mock), \
            patch("pyspark.sql.readwriter.DataFrameReader.format", spark_read_format_mock), \
            patch("pyspark.sql.session.SparkSession.sql", delta_table), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.write_delta_file", write_delta_file_mock):
        valida_tipagem(dbutils_mock, spark_session, table, path_parametros, path_bronze, path_validacao)
