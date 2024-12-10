from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_api.stats.endpoints import leaguedashteamstats
from nba_model.utils.etl import load

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load game log data to PostgreSQL") \
    .getOrCreate()

output_table = "predicting.team_offense_stg"

def extract() -> DataFrame:
    # Get team defense stats, per game averages
    team_response = leaguedashteamstats.LeagueDashTeamStats(per_mode_detailed='PerGame')
    team = team_response.get_data_frames()[0]
    return spark.createDataFrame(team)

if __name__ == "__main__":

    data = extract()
    load(df=data, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()