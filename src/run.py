import logging
import os
import traceback

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from config.logging import LOGGER
from config.spark import spark
from src.utils.schema import get_schema
from src.extract.data_extract import ExtractData
from src.transform.transform import TransformData
from src.transform.encryption_key_handler import generate_encryption_key, get_encryption_key
from src.load.data_writer import DataWriter


def get_data_by_brand(brand: str, data_path, spark_session) -> DataFrame:
    """
    Retrieve data for a specific brand

    :param brand: Input brand
    :param data_path: The path to the data source
    :param spark_session: The Spark session for working with distributed data

    :return: A Spark DataFrame containing the data for the specified brand
    """
    df = ExtractData(path=data_path, spark_session=spark_session).create_dataframe_from_local_path(
        get_schema(brand))

    return df


def get_all_data(data_path, spark_session) -> DataFrame:
    """
    Retrieve all available data

    :param data_path: The path to the data source
    :param spark_session: The Spark session for working with distributed data

    :return: A Spark DataFrame containing all available data
    """
    df = ExtractData(path=data_path, spark_session=spark_session).create_dataframe_from_local_path(
        get_schema())

    return df


def run_tasks(logger: logging.Logger = LOGGER,
              spark_session: SparkSession = spark, schema: StructType = None, brand: str = None,
              data_path: str = None):
    """
    Run a series of tasks, such as getting, transforming, writing, encrypting, and decrypting data

    :param logger: The logger to use for logging messages
    :param spark_session: The Spark session for working with distributed data
    :param schema: The schema for the data
    :param brand: The brand for which the data is processed
    :param data_path: The path to the data source
    """
    brand_list = ["clp", "okay", "spar", "dats", "cogo"]

    try:
        if brand and brand in brand_list:  # if brand is given and is valid
            # check is filename exists
            file_name = ""
            data_filename_list = os.listdir("data/input_data")
            for filename in data_filename_list:
                if brand in filename:
                    file_name = filename
                    break
            data_path = data_path + f"/{file_name}"  # add file name to the file path

            logger.info(f"Getting data for {brand}...")
            df = get_data_by_brand(brand, data_path, spark_session)

            # transform data
            df = TransformData().transform_dataframe(df)

            # add brand column
            logger.info("Adding brand column...")
            df = TransformData.add_brand_column(df, brand)

            df.show(truncate=False)

        elif brand and brand not in brand_list:
            print("Invalid input brand")

        else:
            logger.info(f"Getting all data...")
            df = get_all_data(data_path, spark_session)

            # transform data
            df = df.drop("placeSearchOpeningHours")  # drop column placeSearchOpeningHours
            df = TransformData().transform_dataframe(df)

            # add province column
            logger.info("Adding province column...")
            df = TransformData.add_province_column(df)

            # add one hot encoding handoverServices column
            logger.info("One hot encoding handoverServices...")
            df = TransformData.add_ohe_column(df)

            # encrypt sensitive data
            generate_encryption_key()
            _key = get_encryption_key()
            cols_to_encrypt = ["address_streetName", "address_houseNumber"]

            logger.info(f"Encrypting columns: {', '.join(map(str, cols_to_encrypt))}")
            df = TransformData(key=_key).encrypt_sensitive_data(df, cols_to_encrypt)
            df.show(truncate=False)
            print(f"size: {df.count()} for all encrypted data")

            output_path = "data/output_data/"
            try:
                # write to local path
                DataWriter().write_to_local_path(output_path, df, partition="province")
            except Exception as e:
                logger.error(f"Failed to write to {output_path} . Error: {e}")

            # decrypt sensitive data
            df = TransformData(key=_key).decrypt_sensitive_data(df, cols_to_encrypt)
            df.show(truncate=False)
            print(f"size: {df.count()} for all decrypted data")

    except Exception as e:
        logger.error(traceback.format_exc())

    finally:
        spark.stop()


if __name__ == '__main__':
    brand_name = "spar"
    data_location = "data/input_data"

    run_tasks(data_path=data_location, brand=None)
