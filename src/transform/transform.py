import logging

from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, size, col, explode_outer, lit, udf
from pyspark.sql.types import ArrayType, StructType, StringType
from cryptography.fernet import Fernet

from config.logging import LOGGER


class TransformData:

    def __init__(self, logger: logging.Logger = LOGGER, key: bytes = None):
        """
        Initialize TransformData instance

        :param logger: The logger to use for logging messages
        :param key: The encryption key
        """

        self.logger = logger
        self._key = key

    def transform_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Transform a Spark DataFrame by exploding array and struct types

        :param df: The Spark DataFrame to be transformed

        :return: The transformed Spark DataFrame
        """

        # converting empty arrays "[]" to None values in order to properly explode_outer()
        df = df.withColumn("temporaryClosures",
                           when(size("temporaryClosures") < 1, None).otherwise(col("temporaryClosures")))

        nested_json_flag = True  # to indicate nested structure
        while nested_json_flag:
            self.logger.info("Transforming DataFrame...")
            df = self.explode_dataframe(df)

            # reiterate if there are more nested structures
            nested_json_flag = False
            for col_name in df.schema.names:
                if col_name != "sellingPartners":  # keep sellingPartners as is (array)
                    if isinstance(df.schema[col_name].dataType, ArrayType):
                        nested_json_flag = True
                    elif isinstance(df.schema[col_name].dataType, StructType):
                        nested_json_flag = True

        return df

    @staticmethod
    def explode_dataframe(df) -> DataFrame:
        """
        Transforms dataframe by exploding array and struct types creating new rows

        :param df: The Spark DataFrame to be transformed

        :return: The transformed Spark DataFrame
        """

        column_list = []

        for column_name in df.schema.names:
            # Checking column type is ArrayType
            if isinstance(df.schema[column_name].dataType, ArrayType):
                if df.schema[column_name].name != "sellingPartners":  # skip sellingPartners column
                    df = df.withColumn(column_name, explode_outer(column_name).alias(column_name))
                    column_list.append(column_name)
                else:
                    column_list.append(column_name)
            # Checking column type is StructType
            elif isinstance(df.schema[column_name].dataType, StructType):
                for field in df.schema[column_name].dataType.fields:
                    column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))

            else:
                column_list.append(column_name)

        return df.select(column_list)

    @staticmethod
    def provinces(postal_codes: list) -> dict:
        """
        Map postal codes to provinces

        :param postal_codes: List of postal codes

        :return: Dictionary mapping postal codes to provinces
        """

        province_dict = {}
        for postal_code in postal_codes:
            postal_code_int = int(postal_code)
            if 1000 <= postal_code_int <= 1299:
                province_dict[postal_code] = "Brussels"
            elif 1300 <= postal_code_int <= 1499:
                province_dict[postal_code] = "Waals-Brabant"
            elif (1500 <= postal_code_int <= 1999) or (3000 <= int(postal_code) <= 3499):
                province_dict[postal_code] = "Vlaams-Brabant"
            elif 2000 <= postal_code_int <= 2999:
                province_dict[postal_code] = "Antwerpen"
            elif 3500 <= postal_code_int <= 3999:
                province_dict[postal_code] = "Limburg"
            elif 4000 <= postal_code_int <= 4999:
                province_dict[postal_code] = "Luik"
            elif 5000 <= postal_code_int <= 5999:
                province_dict[postal_code] = "Namen"
            elif (6000 <= postal_code_int <= 6599) or (7000 <= int(postal_code) <= 7999):
                province_dict[postal_code] = "Henegouwen"
            elif 6600 <= postal_code_int <= 6999:
                province_dict[postal_code] = "Luxemburg"
            elif 8000 <= postal_code_int <= 8999:
                province_dict[postal_code] = "West-Vlaanderen"
            elif 9000 <= postal_code_int <= 9999:
                province_dict[postal_code] = "Oost-Vlaanderen"

        return province_dict

    @staticmethod
    def get_province(postal_code, province_dict):
        """
        Get province based on postal code from the province dictionary

        :param postal_code: Postal code
        :param province_dict: Dictionary mapping postal codes to provinces

        :return: The province name or None if not found in the dictionary
        """

        if postal_code in province_dict:
            return province_dict[postal_code]
        else:
            return None

    @staticmethod
    def add_brand_column(df: DataFrame, brand: str) -> DataFrame:
        """
        Add a brand column to the Spark DataFrame

        :param df: The Spark DataFrame
        :param brand: The brand name to be added

        :return: The Spark DataFrame with the added brand column
        """
        return df.withColumn("brand_name", lit(brand))

    @staticmethod
    def add_province_column(df: DataFrame) -> DataFrame:
        """
        Add a province column to the Spark DataFrame based on postal codes (address_postalcode)

        :param df:The Spark DataFrame

        :return: The Spark DataFrame with the added province column
        """

        # get all unique postal codes
        distinct_postal_codes = df.select("address_postalcode").distinct().collect()
        postal_codes_list = [row.address_postalcode for row in distinct_postal_codes]

        province_dict = TransformData.provinces(
            postal_codes_list)  # create dict having all postal codes and their respective province names

        get_province_udf = udf(lambda postal_code: TransformData.get_province(postal_code, province_dict), StringType())

        # add province column based on address_postalcode column
        return df.withColumn("province", get_province_udf(col("address_postalcode")))

    @staticmethod
    def add_ohe_column(df: DataFrame) -> DataFrame:
        """
        Add a one-hot-encoded column for the 'handoverServices' column

        :param df: The Spark DataFrame

        :return: The Spark DataFrame with the added one-hot-encoded column
        """

        # one-hot-encode the handoverServices
        indexer = StringIndexer(inputCol="handoverServices", outputCol="handoverServices_index", handleInvalid="keep")
        indexed_df = indexer.fit(df).transform(df)

        return indexed_df.withColumn("handoverServices_index", col("handoverServices_index").cast("integer"))

    @staticmethod
    def encrypt_data(data, cipher_suite):
        """
        Encrypt data using a cipher suite

        :param data: The data to be encrypted
        :param cipher_suite: The Fernet cipher suite

        :return: The encrypted data as a string or None if input data is None
        """

        if data is not None:
            return cipher_suite.encrypt(data.encode()).decode()
        return None

    @staticmethod
    def decrypt_data(cipher_text, cipher_suite):
        """
        Decrypt data using a cipher suite

        :param cipher_text: The encrypted data
        :param cipher_suite: The Fernet cipher suite

        :return: The decrypted data as a string
        """
        return cipher_suite.decrypt(cipher_text).decode('utf-8')

    def encrypt_sensitive_data(self, df: DataFrame, columns: list) -> DataFrame:
        """
        Encrypt sensitive data in specified columns of the Spark DataFrame

        :param df: The Spark DataFrame containing sensitive data
        :param columns: List of column names containing sensitive data

        :return: The Spark DataFrame with encrypted sensitive data
        """

        cipher_suite = Fernet(self._key)

        encrypt_udf = udf(lambda data: self.encrypt_data(data, cipher_suite), StringType())

        for column in columns:
            df = df.withColumn(column, encrypt_udf(col(column)))

        return df

    def decrypt_sensitive_data(self, df: DataFrame, columns: list) -> DataFrame:
        """
        Decrypt sensitive data in specified columns of the Spark DataFrame

        :param df: The Spark DataFrame containing encrypted sensitive data
        :param columns: List of column names containing encrypted sensitive data

        :return: The Spark DataFrame with decrypted sensitive data
        """

        cipher_suite = Fernet(self._key)

        decrypt_udf = udf(lambda cipher_text: self.decrypt_data(cipher_text, cipher_suite), StringType())

        for column in columns:
            df = df.withColumn(column, decrypt_udf(col(column)))

        return df
