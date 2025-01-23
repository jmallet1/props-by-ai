from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_model.utils.etl import load


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load team season data to PostgreSQL") \
    .getOrCreate()

def extract(file_name: str) -> DataFrame:
    return spark.read.csv(file_name, header=True, inferSchema=True)

if __name__ == "__main__":

    # Load historical CSV file into a PySpark DataFrame
    file_path = 'C:\\Users\\jakem\\wager_wiser\\training_ml_model\\nba_model\\training\\data\\team_season\\Opponent Stats Per Game.csv'

    df = extract(file_path)

    output_table = 'nba.team_season_def_stg'
    load(df=df, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()