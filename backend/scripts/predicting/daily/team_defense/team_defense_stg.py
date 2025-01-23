from pyspark.sql.dataframe import DataFrame
from nba_api.stats.endpoints import leaguedashteamstats
from scripts.utils.etl import load, create_spark_session

def extract(spark) -> DataFrame:
    # Get team defense stats, per game averages
    team_response = leaguedashteamstats.LeagueDashTeamStats(measure_type_detailed_defense='Opponent', per_mode_detailed='PerGame')
    team = team_response.get_data_frames()[0]
    return spark.createDataFrame(team)

def handler():

    output_table = "predicting.team_defense_stg"
    spark = create_spark_session()

    data = extract(spark)
    load(df=data, table=output_table, mode="overwrite")

    spark.stop()

