from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_api.stats.static import players
from nba_model.utils.etl import load

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load game log data to PostgreSQL") \
    .getOrCreate()

output_table = "nba.player_lkp"

def extract() -> DataFrame:

    player_info = players.get_players()

    return spark.createDataFrame(player_info)

if __name__ == "__main__":

    data = extract()
    load(df=data, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()