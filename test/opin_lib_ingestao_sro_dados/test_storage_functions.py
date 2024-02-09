import csv
import os
import unittest.mock as mock
import pytest
from pyspark.sql.types import LongType, StructField, StringType, StructType
from opin_lib_cap_ingestao_sro_dados import storage_functions as fs
import databricks_test

from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment

VALID_FILE_NAME = 'storage_data_test.csv'
VALID_PATH = '/TestTempFiles'


@pytest.fixture
def get_db_utils():
    with databricks_test.session() as dbrickstest:
        return dbrickstest.dbutils


@pytest.mark.usefixtures("spark_session")
def test_read_delta_file_with_empty_data(get_db_utils, spark_session):
    # Arrange
    dbutils = get_db_utils
    dbutils.fs_ls = mock.MagicMock(return_value=[])

    # Act
    data_returned = fs.read_delta_file(dbutils, spark_session, VALID_PATH)

    # Asserts
    assert data_returned is None


@pytest.mark.usefixtures("spark_session")
def test_create_file(spark_session):
    diretorio = "./tempfiles"

    # Verifica se o diretório não existe antes de criá-lo
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)

    with open('./tempfiles/testFileZa.csv', 'w', newline='') as csvfile:
        fieldnames = ['col1', 'col2']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'col1': 1, 'col2': 'A'})
        writer.writerow({'col1': 2, 'col2': 'B'})

    with mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.is_empty") as mock_is_empty:
        mock_is_empty.return_value = False

        data = fs.read_text_file_from_schema(None, spark_session, "csv", True, ",", "./tempfiles/testFileZa.csv")

        assert data.count() == 3
        assert len(data.columns) == 2
        assert data.columns == ['_c0', '_c1']

    spark_session.stop()


def schema_of_test_delta_table():
    return StructType([
        StructField("col1", LongType(), True),
        StructField("col2", StringType(), True)
    ])


@pytest.mark.usefixtures("spark_session")
def test_write_delta_file(tmpdir, spark_session):
    test_dir = str(tmpdir.mkdir("test"))
    test_path = test_dir + "/data"

    data = spark_session.createDataFrame([], schema_of_test_delta_table())

    fs.write_delta_file(data, test_path, fs.OVERWRITE)

    assert len(tmpdir.listdir()) == 1
    assert len(tmpdir.listdir()[0].listdir()) == 0

    spark_session.stop()


@pytest.mark.usefixtures("spark_session")
def test_read_text_file(spark_session):
    with open('./tempfiles/testFile.csv', 'w', newline='') as csvfile:
        fieldnames = ['col1', 'col2']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'col1': 1, 'col2': 'A'})
        writer.writerow({'col1': 2, 'col2': 'B'})

    with mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.is_empty") as mock_is_empty:
        mock_is_empty.return_value = False

        data = fs.read_text_file(None, spark_session, "csv", True, True, ",", "./tempfiles/testFile.csv")

        assert data.count() == 2
        assert len(data.columns) == 2
        assert data.columns == ["col1", "col2"]

    spark_session.stop()


@pytest.mark.usefixtures("spark_session")
def test_read_csv_file(spark_session):
    with open('./tempfiles/testFile.csv', 'w', newline='') as csvfile:
        fieldnames = ['col1', 'col2']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'col1': 1, 'col2': 'A'})
        writer.writerow({'col1': 2, 'col2': 'B'})

    with mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.is_empty") as mock_is_empty:
        mock_is_empty.return_value = False

        data = fs.read_csv_file(None, spark_session, ",", "../tempfiles/testFile.csv", True, "ISO-8859-1")

    spark_session.stop()


@pytest.mark.usefixtures("spark_session")
def test_read_delta_table(spark_session):
    path = 'path'
    delta_table = mock.MagicMock()

    with mock.patch("delta.tables.DeltaTable.forPath", delta_table):
        fs.read_delta_table(spark_session, path)


@pytest.mark.usefixtures("dbutils_helper")
def test_delete_with_files(dbutils_helper):
    env = Environment()
    path = './tempfiles'
    fs.delete(dbutils_helper, path, env)

