# Colruyt Data Pipeline

## Overview

This is a data pipeline with purpose of extracting data, transforming it, and writing it to a local folder and AWS S3 using PySpark.

---
## Prerequisites

To run this project, all you need is to create a virtual environment, import PySpark and Python's Cryptography library.

The input data won't be available in this repo, so consider running these in the `data/input_data` directory:
```
!curl https://ecgplacesmw.colruytgroup.com/ecgplacesmw/v3/nl/places/filter/clp-places > clp-places.json
!curl https://ecgplacesmw.colruytgroup.com/ecgplacesmw/v3/nl/places/filter/okay-places > okay-places.json
!curl https://ecgplacesmw.colruytgroup.com/ecgplacesmw/v3/nl/places/filter/spar-places > spar-places.json
!curl https://ecgplacesmw.colruytgroup.com/ecgplacesmw/v3/nl/places/filter/dats-places > dats-places.json
!curl https://ecgplacesmw.colruytgroup.com/ecgplacesmw/v3/nl/places/filter/cogo-colpnts > cogo-colpnts.json
```

---
## Installations

### Virtual Environment
1. Open your terminal or command prompt. 
2. Navigate to your project directory. Use the `cd` command to change directories to where you want your virtual environment to be located. 
3. Create the virtual environment. Run the following command: ```python -m venv ./.venv```
4. Activate your virtual environment:

    On MacOs:
    `source .venv/bin/activate`
    
    On Windows:
    `.\.venv\Scripts\activate`

5. With your virtual environment still active, install the following:
    #### PySpark
    ```pip install pyspark```
    
    #### Cryptography Library
    ```pip install cryptography```

---

## Project & Code Structure

This project consists of multiple modules with their own purposes:
- The `config/` module contains project configurations namely `spark.py` and `logging.py`, and has a sub-folder having an encryption key (for encryption and decryption of sensitive data)
- The `logs/` folder contains the project logs displaying messages from the project run
- The `src/` module contains all other modules required for the pipeline
- `data/` folder contains sub-folders `input_data/` and `output_data/` for retrieving data and writing data to a location respectively
- `extract/` module contains an `extract_data.py` file which contains the ExtractData class for retrieving data
- `transform/` module contains a `transform_data.py` file (which defines the TransformData class with various data transformation methods) and an `encryption_key_handler.py` file (that generates and retrieves an encryption key) 
- `load/` module contains a `data_writer.py` that implements the DataWriter class for writing data to a local path.
- `utils/` module containing a `schema.py` file having a method to get the correct schema for the data extracted
- `run.py` to run all tasks

---
## Data Schemas

There are 3 different schemas in `schema.py`:
- default_schema: The default schema containing all columns from all brands. Used for brands "clp", "okay", and "cogo".
- dats_schema: The schema specific to the "dats" brand.
- spar_schema: The schema specific to the "spar" brand.

---

## Encryption

The data extracted contains sensitive data which should be treated to abide by GDPR.

There are two columns ("address_streetName", "address_houseNumber") that need to be encrypted to prevent unauthorized individuals from reading it.

This encryption was achieved by using Python's Cryptography library which uses symmetric encryption. So, the same key used to encrypt can be used to decrypt. This was implemented as follows:

1. The imports needed:

    ```
    from pyspark.sql import SparkSession
    from cryptography.fernet import Fernet
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    ```
2. Generate an encryption key, store it (to decrypt), and retrieve it to encrypt:

     `encryption_key_handler.py`
    ```
   def generate_encryption_key():
    """
    Generate a new encryption key and save it as a file

    note: The encryption key is generated using the Fernet symmetric encryption algorithm
    """

    LOGGER.info("Generating encryption key...")
    key = Fernet.generate_key()  # generate encryption key
    with open("../config/encryption_key/encryption_key.key", "wb") as key_file:
        key_file.write(key)


    def get_encryption_key() -> bytes:
        """
        Retrieve the encryption key from the file
    
        :return: The encryption key
        """
    
        with open("../config/encryption_key/encryption_key.key", "rb") as key_file:
            key = key_file.read()
        return key
   ```
3. A cipher suite is created to encrypt the data using the generated key. This cipher suite is basically an algorithm that will be used for encryption and decryption operations:
     
    `transform.py`
    
    ```
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
    ```
    ```
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

    ```

