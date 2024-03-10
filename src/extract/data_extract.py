import logging

from pyspark.sql import SparkSession, DataFrame

from config.logging import LOGGER


class ExtractData:

    def __init__(self, path: str, logger: logging.Logger = LOGGER, spark_session: SparkSession = None):
        """
        Initialize ExtractData instance

        :param path: The path to the data source
        :param logger: The logger to use for logging messages
        :param spark_session: The Spark session for working with distributed data
        """
        self.path = path
        self.logger = logger
        self.spark_session = spark_session

    def create_dataframe_from_local_path(self, schema=None) -> DataFrame:
        """
        Create a Spark DataFrame from a local file path

        :param schema: The schema for the data

        :return: A Spark DataFrame created from the local file path
        """
        self.logger.info(f"Getting data from: {self.path}")
        df = self.spark_session.read.json(self.path, schema=schema).cache()

        return df
