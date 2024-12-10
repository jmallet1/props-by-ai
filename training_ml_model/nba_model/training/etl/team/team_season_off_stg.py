from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_model.utils.etl import load

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load team season data to PostgreSQL") \
    .getOrCreate()

def extract(file_name: str) -> DataFrame:
    return spark.read.csv(file_name, header=True, inferSchema=True)

def transform(df: DataFrame) -> DataFrame:

    df = df.drop("lg", "playoffs")

    return df

if __name__ == "__main__":

    # Load CSV file into a PySpark DataFrame
    file_path = 'C:\\Users\\jakem\\wager_wiser\\training_ml_model\\nba_model\\training\\data\\team_season\\Team Stats Per Game.csv'

    df = extract(file_path)
    transformed_df = transform(df)

    output_table = 'nba.team_season_off_stg'
    load(df=transformed_df, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()