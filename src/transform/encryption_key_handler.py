from cryptography.fernet import Fernet

from config.logging import LOGGER


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
