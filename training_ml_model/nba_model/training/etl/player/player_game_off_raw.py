from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, when, lag, substring
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load team season data to PostgreSQL") \
    .getOrCreate()

# Database connection properties
db_url = "jdbc:postgresql://localhost:5432/postgres"
input_table = "nba.player_game_off_stg"  # Name of the input table in PostgreSQL
output_table = "nba.player_game_off_raw"  # Name of the output table in PostgreSQL
db_properties = {
    "user": "postgres",
    "password": "football0103J",
    "driver": "org.postgresql.Driver"
}

def extract_by_season(season: str) -> DataFrame:
    """Extracts data for a specific season from a PostgreSQL table."""
    query = f"(SELECT * FROM {input_table} WHERE season_year = {season})"
    return spark.read.jdbc(url=db_url, table=query, properties=db_properties)

def transform(df: DataFrame) -> DataFrame:

    df = df.withColumn("away_flag", when(col("matchup").contains("@"), 1).otherwise(0))

    # Define the window specification for each player within each season and order by date of game
    window_spec = Window.partitionBy("player_name", "season_year").orderBy("game_date_parsed")

    # Calculate b2b_flag by checking the difference in days between consecutive games
    df = df.withColumn(
        "b2b_flag",
        when(datediff(col("game_date_parsed"), lag("game_date_parsed", 1).over(window_spec)) == 1, 1).otherwise(0)
    )

    df = df.withColumn("matchup", substring(df['matchup'], -3, 3))
    return df


def load(df: DataFrame, table: str):
    """Loads data into a PostgreSQL table."""
    df.write.jdbc(url=db_url, table=table, mode="append", properties=db_properties)


if __name__ == "__main__":

    # Run ETL process for each season from 1950 to 2022
    seasons = [str(year) for year in range(1947, 2024)]  # List of seasons from 1950 to 2022

    for season in seasons:
        data = extract_by_season(season)
        data_transformed = transform(data)
        load(data_transformed, output_table)

    # Stop Spark session
    spark.stop()