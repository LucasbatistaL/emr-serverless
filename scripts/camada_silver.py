from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from utils import (
    create_spark_session, read_csv, filter_invalid_times,
    write_parquet, add_validation
)

BUCKET = "[BUCKET_NAME]"
INPUT_PATH = f"s3://{BUCKET}/data/fedex.csv"
OUTPUT_PATH = f"s3://{BUCKET}/output/fedex_silver/"


# Converte colunas de horário (inteiro) para timestamp combinando com Year/Month/Day
def convert_timestamps(df: DataFrame) -> DataFrame:
    for col_name in ["Actual_Shipment_Time", "Planned_Shipment_Time", "Planned_Delivery_Time"]:
        df = df.withColumn(col_name,
            to_timestamp(
                concat_ws("-", col("Year"), col("Month"), col("DayofMonth"),
                    lpad(col(col_name), 4, "0")),
                "yyyy-M-d-HHmm"))
    return df


# Calcula a diferença em minutos entre o horário real e o planejado de envio
def add_travel_time(df: DataFrame) -> DataFrame:
    return df.withColumn("Travel_Time_Real",
        ((unix_timestamp(col("Actual_Shipment_Time")) -
          unix_timestamp(col("Planned_Shipment_Time"))) / 60).cast("integer"))


# Cria identificador de rota concatenando origem e destino (ex: IAD-TPA)
def add_route_id(df: DataFrame) -> DataFrame:
    return df.withColumn("Route_ID", concat_ws("-", col("Source"), col("Destination")))


# Aplica todas as validações de qualidade dos dados
def add_quality_validations(df: DataFrame) -> DataFrame:
    # Distância deve ser positiva
    df = add_validation(df, "validate_distance", col("Distance") > 0)
    # Tempo de viagem planejado não pode ser negativo
    df = add_validation(df, "validate_traveltime", col("Planned_TimeofTravel") >= 0)
    # Atrasos fora de -300 a 1440 minutos são outliers
    df = add_validation(df, "outlier_delay",
        (col("Shipment_Delay") >= -300) & (col("Shipment_Delay") <= 1440), "normal", "outlier")
    # Status de entrega deve ser 0 ou 1
    df = add_validation(df, "validate_status", col("Delivery_Status").isin(0, 1))
    # Carrier deve ser WN (Southwest Airlines)
    df = add_validation(df, "validate_carrier", col("Carrier_Name") == "WN")
    # Código de origem deve ter 3 caracteres (padrão IATA)
    df = add_validation(df, "validate_source", length(col("Source")) == 3)
    # Código de destino deve ter 3 caracteres (padrão IATA)
    df = add_validation(df, "validate_destination", length(col("Destination")) == 3)
    # Verifica se a diferença entre horários planejados bate com o tempo de viagem planejado
    df = add_validation(df, "validate_planned_time",
        abs((unix_timestamp(col("Planned_Delivery_Time")) - unix_timestamp(col("Planned_Shipment_Time"))) /
        60 - col("Planned_TimeofTravel")) <= 2, "consistente", "inconsistente")
    # Verifica se o tempo real de viagem é consistente com o delay reportado
    df = add_validation(df, "validate_delay",
        abs(col("Travel_Time_Real") - col("Shipment_Delay")) <= 2, "consistente", "inconsistente")
    return df


# Categoriza o atraso em faixas: OnTime, Minor (até 15min), Moderate (até 60min), Severe (acima)
def add_delay_category(df: DataFrame) -> DataFrame:
    return df.withColumn("Delay_Category",
        when(col("Shipment_Delay") <= 0, "OnTime")
        .when(col("Shipment_Delay") <= 15, "Minor")
        .when(col("Shipment_Delay") <= 60, "Moderate")
        .otherwise("Severe"))


# Função principal que orquestra o pipeline: leitura → transformações → escrita
def main():
    spark = create_spark_session("Airliness")

    df = read_csv(spark, INPUT_PATH)
    df = filter_invalid_times(df)
    df = convert_timestamps(df)
    df = add_travel_time(df)
    df = add_route_id(df)
    df = add_quality_validations(df)
    df = add_delay_category(df)

    write_parquet(df, OUTPUT_PATH, ["Year", "Month"])

    spark.stop()


if __name__ == "__main__":
    main()
