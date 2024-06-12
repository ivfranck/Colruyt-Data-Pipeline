import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '../config/credentials.cfg'))


_AWS_ACCESS_KEY = config.get('aws', 'access_key')
_AWS_SECRET_KEY = config.get('aws', 'secret_key')

S3_BUCKET = config.get('s3', 'bucket')
SERVICE_NAME = config.get('s3', 'service')
REGION_NAME = config.get('s3', 'region')

INPUT_PATH = config.get('paths', 'input_path')
OUTPUT_PATH = config.get('paths', 'local_output_path')
S3_PATH = config.get('paths', 's3_path')
