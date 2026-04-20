"""
Pre-download Kafka and Snowflake JARs for Spark Streaming.
Creates a Spark session with the required packages - Spark resolves and
downloads JARs from Maven on first run. Cached to ~/.ivy2/cache.
"""
from pyspark.sql import SparkSession

PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "net.snowflake:snowflake-jdbc:3.13.30,"
    "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4"
)

print("Downloading Kafka/Snowflake JARs (first run may take a few minutes)...")
spark = SparkSession.builder \
    .appName("JarDownload") \
    .config("spark.jars.packages", PACKAGES) \
    .getOrCreate()

print("JARs downloaded successfully.")
spark.stop()
