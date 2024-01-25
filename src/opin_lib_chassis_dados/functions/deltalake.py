from pyspark.sql import DataFrame, SparkSession


class DeltaLakeFunctions:

    @staticmethod
    def read_delta_stream(spark: SparkSession, format: str, path: str):
        return spark.readStream.format(format).load(path)

    @staticmethod
    def write_delta_stream(df: DataFrame, format: str, path: str, checkpoint_location: str):
        return df \
            .writeStream \
            .format(format) \
            .option("path", path) \
            .option("checkpointLocation", checkpoint_location) \
            .start() \
            .awaitTermination()