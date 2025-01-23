from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_api.stats.endpoints import leaguedashteamstats
from scripts.utils.etl import load, create_spark_session


def extract(spark) -> DataFrame:
    # Get team defense stats, per game averages
    team_response = leaguedashteamstats.LeagueDashTeamStats(per_mode_detailed='PerGame')
    team = team_response.get_data_frames()[0]
    return spark.createDataFrame(team)

def handler():

    spark = create_spark_session()
    output_table = "predicting.team_offense_stg"

    data = extract(spark)
    load(df=data, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()