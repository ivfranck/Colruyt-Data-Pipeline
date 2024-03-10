import logging
import os

from pyspark.sql import DataFrame

from config.logging import LOGGER


class DataWriter:
    def __init__(self, logger: logging.Logger = LOGGER):
        """
        Initialize DataWriter instance

        :param logger: The logger to use for logging messages
        """
        self.logger = logger

    def write_to_local_path(self, output_path: str, df: DataFrame, partition: str):
        """
        Write a Spark DataFrame to a local path in Parquet format with partitioning

        :param output_path: The path where the data will be written
        :param df: The Spark DataFrame to be written
        :param partition: The column by which to partition the data

        note: The data will be written in Parquet format, and any existing data at the specified output_path will be
        overwritten.
        """

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        self.logger.info(f"Writing to path: {output_path}")
        self.logger.info(f"Partitioning by {partition}")
        df.write.partitionBy(partition).parquet(output_path, mode="overwrite")
