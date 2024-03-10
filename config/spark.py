from pyspark.sql import SparkSession

# spark setup
spark = (
    SparkSession
    .builder
    .appName("cg-pyspark-assignment")
    .master("local")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .getOrCreate()
)
