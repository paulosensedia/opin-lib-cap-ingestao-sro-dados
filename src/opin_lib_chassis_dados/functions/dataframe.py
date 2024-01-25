from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType


class DataFrameFunctions:

    @staticmethod
    def parse_json(df: DataFrame, json_column_from: str, json_column_to: str, schema: StructType) -> DataFrame:
        return df.withColumn(json_column_to, from_json(
            col(json_column_from).cast("string"), schema))

    @staticmethod
    def rename_columns(df: DataFrame, columns_name: dict) -> DataFrame:
        for column, new_name in columns_name.items():
            df = df.withColumnRenamed(column, new_name)

        return df

    @staticmethod
    def cast_columns(df: DataFrame, columns_types: dict) -> DataFrame:
        for column, type in columns_types.items():
            df = df.withColumn(column, col(column).cast(type))

        return df