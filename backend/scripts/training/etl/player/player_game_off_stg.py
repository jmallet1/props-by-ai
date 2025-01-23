from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, month, to_date
from pyspark.sql.dataframe import DataFrame
from nba_model.utils.etl import load


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load game log data to PostgreSQL") \
    .getOrCreate()

def extract(file_path: str) -> DataFrame:
    return spark.read.csv(file_path, header=True, inferSchema=True)

def transform(df: DataFrame) -> DataFrame:

    # Parse game_date column
    df = df.withColumn("game_date_parsed", to_date(col("game_date"), "MMM dd, yyyy"))

    # Add a column for the NBA season
    df = df.withColumn(
        "season_year",
        when(month(col("game_date_parsed")) >= 10, year(col("game_date_parsed")) + 1)
        .otherwise(year(col("game_date_parsed")))
    )

    df = df.drop("season", "game_date", "_c0", "game_id", "video_available")
    return df


if __name__ == "__main__":

    # Load CSV file into a PySpark DataFrame
    file_path = 'C:\\Users\\jakem\\wager_wiser\\training_ml_model\\nba_model\\training\\data\\game_logs\\box_score_stats_1950_2022.csv'
    data = extract(file_path)

    data_transformed = transform(data)
    output_table = 'nba.player_game_off_stg'

    load(df=data_transformed, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()