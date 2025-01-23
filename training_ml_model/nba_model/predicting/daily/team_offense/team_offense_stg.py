from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_api.stats.endpoints import leaguedashteamstats
from training_ml_model.nba_model.utils.etl import load

# Define the path to the JAR file inside the container
jdbc_jar_path = "/opt/airflow/jars/postgresql-42.7.4.jar"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load game log data to PostgreSQL") \
    .config("spark.jars", jdbc_jar_path) \
    .getOrCreate()

output_table = "predicting.team_offense_stg"

def extract() -> DataFrame:
    # Get team defense stats, per game averages
    team_response = leaguedashteamstats.LeagueDashTeamStats(per_mode_detailed='PerGame')
    team = team_response.get_data_frames()[0]
    return spark.createDataFrame(team)

def handler():
    
    data = extract()
    load(df=data, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()