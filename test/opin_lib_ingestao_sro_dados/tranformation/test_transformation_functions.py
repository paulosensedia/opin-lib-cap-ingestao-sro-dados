import pytest
from pyspark import Row
from opin_lib_cap_ingestao_sro_dados.functions.transformation.transformation_functions import clean_special_char, \
    format_date_time, add_ramo_susep, rename_columns
from test.opin_lib_ingestao_sro_dados.tranformation.transformation_mock_schemas import schema_multivalues_fields, \
    schema_ramos_fields
from test.opin_lib_ingestao_sro_dados.tranformation.transformation_mock_values import TestDataGenericValuesEnum, \
    TestDataRamosValuesEnum

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


@pytest.mark.usefixtures("spark_session")
def test_clean_special_char(spark_session):

    schema = schema_multivalues_fields()
    mock = spark_session.createDataFrame([mock_row], schema)

    df_transformado = clean_special_char(mock)

    assert df_transformado.count() >= 1


@pytest.mark.usefixtures("spark_session")
def test_format_date_time(spark_session):

    schema = schema_multivalues_fields()
    mock = spark_session.createDataFrame([mock_row], schema)

    df_transformado = format_date_time(mock, 'DATA')

    assert df_transformado.count() >= 1


@pytest.mark.usefixtures("spark_session")
def test_rename_columns(spark_session):

    schema = schema_multivalues_fields()
    mock = spark_session.createDataFrame([mock_row], schema)

    df_transformado = rename_columns(mock)

    assert df_transformado.count() >= 1


@pytest.mark.usefixtures("spark_session")
def test_add_ramo_susep(spark_session):

    schema = schema_multivalues_fields()
    mock = spark_session.createDataFrame([mock_row], schema)

    row_ramo = Row(
        TestDataRamosValuesEnum.ID_RAMO.value,
        TestDataRamosValuesEnum.C_RAMO.value,
        TestDataRamosValuesEnum.DESC_RAMO.value,
        TestDataRamosValuesEnum.R_RAMO.value,
        TestDataRamosValuesEnum.R_GRP_RAMO.value
    )

    schema_ramo = schema_ramos_fields()
    mock_ramo = spark_session.createDataFrame([row_ramo], schema_ramo)

    df_transformado = add_ramo_susep(mock, mock_ramo)
    df_transformado.show(5, False)
    assert df_transformado.count() >= 1