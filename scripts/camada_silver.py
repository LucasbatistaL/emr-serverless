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

df_bronze = df_bronze \
    .withColumn("Actual_Shipment_Time",
        to_timestamp(
            concat_ws("-", col("Year"), col("Month"), col("DayofMonth"),
                lpad(col("Actual_Shipment_Time"), 4, "0")),
            "yyyy-M-d-HHmm")) \
    .withColumn("Planned_Shipment_Time",
        to_timestamp(
            concat_ws("-", col("Year"), col("Month"), col("DayofMonth"),
                lpad(col("Planned_Shipment_Time"), 4, "0")),
            "yyyy-M-d-HHmm")) \
    .withColumn("Planned_Delivery_Time",
        to_timestamp(
            concat_ws("-", col("Year"), col("Month"), col("DayofMonth"),
                lpad(col("Planned_Delivery_Time"), 4, "0")),
            "yyyy-M-d-HHmm"))

df_bronze = df_bronze.withColumn("Travel_Time_Real",
    ((unix_timestamp(col("Actual_Shipment_Time")) -
      unix_timestamp(col("Planned_Shipment_Time"))) / 60).cast("integer"))

df_bronze = df_bronze.withColumn("Route_ID",
    concat_ws("-", col("Source"), col("Destination"))
)

df_bronze = df_bronze \
    .withColumn("validate_distance",
        when(col("Distance") > 0, "valido").otherwise("invalido")) \
    .withColumn("validate_traveltime",
        when(col("Planned_TimeofTravel") >= 0, "valido").otherwise("invalido")) \
    .withColumn("outlier_delay",
        when((col("Shipment_Delay") < -300) | (col("Shipment_Delay") > 1440), "outlier").otherwise("normal")) \
    .withColumn("validate_status",
        when(col("Delivery_Status").isin(0, 1), "valido").otherwise("invalido"))

df_bronze = df_bronze.withColumn("Delay_Category",
    when(col("Shipment_Delay") <= 0, "OnTime")
    .when(col("Shipment_Delay") <= 15, "Minor")
    .when(col("Shipment_Delay") <= 60, "Moderate")
    .otherwise("Severe")
)

df_bronze = df_bronze.withColumn("validate_carrier",
    when(col("Carrier_Name") == "WN", "valido").otherwise("invalido"))

df_bronze = df_bronze \
    .withColumn("validate_source",
        when(length(col("Source")) == 3, "valido").otherwise("invalido")) \
    .withColumn("validate_destination",
        when(length(col("Destination")) == 3, "valido").otherwise("invalido"))

df_bronze = df_bronze.withColumn("validate_planned_time",
    when(abs((unix_timestamp(col("Planned_Delivery_Time")) - unix_timestamp(col("Planned_Shipment_Time"))) /
     60 - col("Planned_TimeofTravel")) <= 2, "consistente")
    .otherwise("inconsistente"))

df_bronze = df_bronze.withColumn("validate_delay",
    when(abs(col("Travel_Time_Real") - col("Shipment_Delay"))
     <= 2, "consistente").otherwise("inconsistente"))

df_bronze.write \
    .mode("overwrite") \
    .partitionBy("Year", "Month") \
    .parquet("s3://[BUCKET_NAME]/output/fedex_silver/")

spark.stop()
