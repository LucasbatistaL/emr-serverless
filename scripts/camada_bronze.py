from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Airliness") \
    .getOrCreate()

df_bronze = spark.read.csv(
    "s3://[BUCKET_NAME]/data/fedex.csv",
    header=True, inferSchema=True, sep=";", nullValue="NA"
)

df_bronze = df_bronze.filter(
    (col("Actual_Shipment_Time") >= 0) & (col("Actual_Shipment_Time") <= 2359) &
    (col("Planned_Shipment_Time") >= 0) & (col("Planned_Shipment_Time") <= 2359) &
    (col("Planned_Delivery_Time") >= 0) & (col("Planned_Delivery_Time") <= 2359)
)

df_bronze.write \
    .mode("overwrite") \
    .partitionBy("Year", "Month") \
    .parquet("s3://[BUCKET_NAME]/output/fedex_bronze/")

spark.stop()
