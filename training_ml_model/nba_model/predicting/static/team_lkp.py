from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_api.stats.static import teams
from nba_model.utils.etl import load


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load game log data to PostgreSQL") \
    .getOrCreate()

output_table = "nba.team_lkp"

# Get team info from NBA API
def extract() -> DataFrame:
    teams_info = teams.get_teams()
    return spark.createDataFrame(teams_info)

if __name__ == "__main__":

    data = extract()
    load(df=data, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()