from typing import Union, List

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField


class DeltaTableFunctions:

    @staticmethod
    def create_empty_deltatable(spark: SparkSession, columns: Union[StructType, List[StructField]], location_tb: str):
        return DeltaTable \
            .create(spark) \
            .addColumns(columns) \
            .location(location_tb) \
            .execute()
