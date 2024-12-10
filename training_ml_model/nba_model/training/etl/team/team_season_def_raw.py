from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_model.utils.etl import extract, load


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load team season data to PostgreSQL") \
    .getOrCreate()

input_table = "nba.team_season_def_stg"  # Name of the input table in PostgreSQL
output_table = "nba.team_season_def_raw"  # Name of the output table in PostgreSQL

def transform(df: DataFrame) -> DataFrame:

    df = df.drop("lg", "playoffs", "g")
    return df

if __name__ == "__main__":

    # Load historical team season defense data into raw table
    query = f"SELECT * FROM {input_table}"

    data = extract(query=query, spark=spark)
    data_transformed = transform(data)
    load(df=data_transformed, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()