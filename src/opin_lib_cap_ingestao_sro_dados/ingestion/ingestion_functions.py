from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from opin_lib_cap_ingestao_sro_dados import storage_functions as sf
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment


def text_file_ingestion(dbutils, spark: SparkSession, input_path: str,
                        output_path: str, file_format: str, has_header: bool,
                        infer_schema: bool, delimiter: str, charset: str = "UTF-8"):
    if not sf.is_empty(dbutils, input_path):
        env = Environment()
        data = sf.read_text_file(dbutils, spark, file_format, has_header, infer_schema,
                                 delimiter, input_path, charset)
        sf.write_delta_file(data, output_path, sf.OVERWRITE)
        sf.delete(dbutils, input_path, env)


def text_file_from_schema_ingestion(dbutils, spark: SparkSession, input_path: str,
                                    output_path: str, file_format: str, schema: StructType,
                                    delimiter: str):
    if not sf.is_empty(dbutils, input_path):
        env = Environment()
        data = sf.read_text_file_from_schema(dbutils, spark, file_format, schema, delimiter, input_path)
        sf.write_delta_file(data, output_path, sf.OVERWRITE)
        sf.delete(dbutils, input_path, env)
