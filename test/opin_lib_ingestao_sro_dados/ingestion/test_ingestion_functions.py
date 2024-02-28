import csv
import os
from unittest import mock
import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_cap_ingestao_sro_dados.functions.ingestion import ingestion_functions as ifu


@pytest.mark.usefixtures("spark_session")
def test_text_file_ingestion(spark_session):
    env = Environment()
    input_path = '../tempfiles/testFile.csv'
    output_path = "../tempfiles/outtestFile.csv"
    diretorio = "../tempfiles"

    # Verifica se o diretório não existe antes de criá-lo
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)

    with open(input_path, 'w', newline='') as csvfile:
        fieldnames = ['col1', 'col2']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'col1': 1, 'col2': 'A'})
        writer.writerow({'col1': 2, 'col2': 'B'})

    with mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.is_empty") as mock_is_empty, \
            mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.write_delta_file") as mock_delta_file, \
            mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.delete") as mock_delete:
        mock_is_empty.return_value = False
        mock_delta_file.return_value = True
        mock_delete.return_value = True

        ifu.text_file_ingestion(None, spark_session, input_path, output_path, 'csv', True, False, '\t', "ISO-8859-1")


def schema_of_test_delta_table():
    return StructType([
        StructField("col1", LongType(), True),
        StructField("col2", StringType(), True)
    ])


@pytest.mark.usefixtures("spark_session")
def test_text_file_from_schema_ingestion(spark_session):
    env = Environment()
    input_path = '../tempfiles/testFile.csv'
    output_path = "../tempfiles/outtestFile.csv"
    diretorio = "../tempfiles"

    # Verifica se o diretório não existe antes de criá-lo
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)

    with open(input_path, 'w', newline='') as csvfile:
        fieldnames = ['col1', 'col2']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'col1': 1, 'col2': 'A'})
        writer.writerow({'col1': 2, 'col2': 'B'})

    with mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.is_empty") as mock_is_empty, \
            mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.write_delta_file") as mock_delta_file, \
            mock.patch("opin_lib_cap_ingestao_sro_dados.storage_functions.delete") as mock_delete:
        mock_is_empty.return_value = False
        mock_delta_file.return_value = True
        mock_delete.return_value = True

        ifu.text_file_from_schema_ingestion(None, spark_session, input_path, output_path, 'csv',
                                            schema_of_test_delta_table, '\t')

