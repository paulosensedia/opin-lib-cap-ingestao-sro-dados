from unittest.mock import MagicMock, patch

import pytest
from pyspark import Row

from opin_lib_cap_ingestao_sro_dados.ingestion import ramo_susep as rs
from test.opin_lib_ingestao_sro_dados.tranformation.transformation_mock_schemas import schema_ramos_fields, \
    schema_multivalues_fields
from test.opin_lib_ingestao_sro_dados.tranformation.transformation_mock_values import TestDataRamosValuesEnum, \
    TestDataGenericValuesEnum


def test_normalize_accented_word():
    value = 'ÁÉ'
    rs.normalize_accented_word(value)


@pytest.mark.usefixtures("spark_session")
def test_upper_columns(spark_session):
    row_ramo = Row(
        TestDataRamosValuesEnum.ID_RAMO.value,
        TestDataRamosValuesEnum.C_RAMO.value,
        TestDataRamosValuesEnum.DESC_RAMO.value,
        TestDataRamosValuesEnum.R_RAMO.value,
        TestDataRamosValuesEnum.R_GRP_RAMO.value
    )

    schema_ramos = schema_ramos_fields()
    mock_ramo = spark_session.createDataFrame([row_ramo], schema_ramos)

    df_transformado = rs.upper_columns(mock_ramo)
    df_transformado.show(5, False)
    assert df_transformado.count() >= 1


def test_normalize_accented(spark_session):
    row_ramo = Row(
        TestDataRamosValuesEnum.ID_RAMO.value,
        TestDataRamosValuesEnum.C_RAMO.value,
        TestDataRamosValuesEnum.DESC_RAMO.value,
        TestDataRamosValuesEnum.R_RAMO.value,
        TestDataRamosValuesEnum.R_GRP_RAMO.value
    )

    schema_ramos = schema_ramos_fields()
    mock_ramo = spark_session.createDataFrame([row_ramo], schema_ramos)

    df_transformado = rs.normalize_accented(mock_ramo)
    df_transformado.show(5, False)
    assert df_transformado.count() >= 1


@pytest.mark.usefixtures("spark_session")
def test_ingestion_ramo_susep(spark_session):
    mock_row = Row(
        TestDataGenericValuesEnum.ID_PRODUTO.value,
        TestDataGenericValuesEnum.NOME_PRODUTO.value,
        TestDataGenericValuesEnum.ID_TIPO_PRODUTO.value,
        TestDataGenericValuesEnum.TIPO_TIPO_PRODUTO.value,
        TestDataGenericValuesEnum.PROCEDENCIA.value,
        TestDataGenericValuesEnum.DATA.value,
        TestDataGenericValuesEnum.LIMITE_MAX_IDADE_ANOS.value,
        TestDataGenericValuesEnum.LIMITE_MIN_IDADE_ANOS.value,
        TestDataGenericValuesEnum.CEP_INICIO.value,
        TestDataGenericValuesEnum.CEP_FIM.value,
        TestDataGenericValuesEnum.ZCH_RAMO.value
    )
    schema = schema_multivalues_fields()
    df = spark_session.createDataFrame([mock_row], schema)

    read_csv_file_mock = MagicMock(return_value=df)
    write_delta_file_mock = MagicMock()
    upper_columns_mock = MagicMock()
    input_path = 'input_path'
    output_path = 'output_path'

    with patch("opin_lib_cap_ingestao_sro_dados.storage_functions.read_csv_file", read_csv_file_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.storage_functions.write_delta_file", write_delta_file_mock), \
            patch("opin_lib_cap_ingestao_sro_dados.ingestion.ramo_susep.upper_columns", upper_columns_mock):
        rs.ingestion(None, spark_session, input_path, output_path)


