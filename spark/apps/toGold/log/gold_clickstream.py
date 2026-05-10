from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Process Clickstream Log Data and Write to Hive") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("log_level", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("outcome", StringType(), True)
])

clickstream_log_path = "hdfs://namenode:9000/data/silver/click"

df_clickstream = spark.read \
    .schema(schema) \
    .option("mode", "PERMISSIVE") \
    .csv(clickstream_log_path)

df_clickstream = df_clickstream.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
)

df_clickstream.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("logs.Ecom_clickstream")

spark.stop()
