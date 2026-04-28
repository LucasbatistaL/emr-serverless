from utils import create_spark_session, read_csv, filter_invalid_times, write_parquet

BUCKET = "[BUCKET_NAME]"
INPUT_PATH = f"s3://{BUCKET}/data/fedex.csv"
OUTPUT_PATH = f"s3://{BUCKET}/output/fedex_bronze/"


# Função principal: leitura do CSV bruto → filtro inicial → escrita em Parquet
def main():
    spark = create_spark_session("Airliness")

    df = read_csv(spark, INPUT_PATH)
    df = filter_invalid_times(df)

    write_parquet(df, OUTPUT_PATH, ["Year", "Month"])

    spark.stop()


if __name__ == "__main__":
    main()
