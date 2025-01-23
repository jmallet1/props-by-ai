from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_api.stats.static import teams
from scripts.utils.etl import load, create_spark_session


# Get team info from NBA API
def extract(spark) -> DataFrame:
    teams_info = teams.get_teams()
    return spark.createDataFrame(teams_info)

def handler():

    spark = create_spark_session()
    output_table = "nba.team_lkp"

    data = extract(spark)
    load(df=data, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()