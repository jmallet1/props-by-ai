from pyspark.sql import SparkSession
from nba_model.utils.etl import extract, load

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load team season data to PostgreSQL") \
    .getOrCreate()

input_table = "nba.team_season_off_stg"  # Name of the input table in PostgreSQL
output_table = "nba.team_season_off_raw"  # Name of the output table in PostgreSQL

if __name__ == "__main__":

    query = f"SELECT * FROM {input_table}"

    data = extract(query=query, spark=spark)
    load(df=data, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()