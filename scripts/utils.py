from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *


# Cria a sessão Spark com o nome da aplicação
def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# Lê um arquivo CSV do S3 com separador ; e tratamento de nulos
def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, inferSchema=True, sep=";", nullValue="NA")


# Remove registros com horários fora do intervalo válido (0000-2359)
def filter_invalid_times(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("Actual_Shipment_Time") >= 0) & (col("Actual_Shipment_Time") <= 2359) &
        (col("Planned_Shipment_Time") >= 0) & (col("Planned_Shipment_Time") <= 2359) &
        (col("Planned_Delivery_Time") >= 0) & (col("Planned_Delivery_Time") <= 2359)
    )


# Grava o DataFrame como Parquet particionado no S3
def write_parquet(df: DataFrame, path: str, partition_cols: list) -> None:
    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)


# Função genérica de validação: cria uma coluna baseado na condição
def add_validation(df: DataFrame, col_name: str, condition, valid="valido", invalid="invalido") -> DataFrame:
    return df.withColumn(col_name, when(condition, valid).otherwise(invalid))
