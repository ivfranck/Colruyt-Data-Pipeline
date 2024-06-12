from pyspark.sql import SparkSession

from src.constants import _AWS_ACCESS_KEY, _AWS_SECRET_KEY

# spark setup
spark = (
    SparkSession
    .builder
    .appName("cg-pyspark-assignment")
    .master("local")
    .config("spark.hadoop.fs.s3a.access.key", _AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", _AWS_SECRET_KEY)
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.0")
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a'
                                                            '.SimpleAWSCredentialsProvider')
    .config("spark.sql.repl.eagerEval.enabled", True)
    .getOrCreate()
)
