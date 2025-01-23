from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_api.stats.endpoints import leaguedashteamstats
from training_ml_model.nba_model.utils.etl import load

def extract(spark) -> DataFrame:
    # Get team defense stats, per game averages
    team_response = leaguedashteamstats.LeagueDashTeamStats(measure_type_detailed_defense='Opponent', per_mode_detailed='PerGame')
    team = team_response.get_data_frames()[0]
    return spark.createDataFrame(team)

def handler():
    output_table = "predicting.team_defense_stg"

    # Define the path to the JAR file inside the container
    jdbc_jar_path = "/opt/airflow/jars/postgresql-42.7.4.jar"

    print("INITIALIZING SPARK")
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Load game log data to PostgreSQL") \
        .config("spark.jars", jdbc_jar_path) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    print("EXTRACTING")
    data = extract(spark)
    print("LOADING")
    load(df=data, table=output_table, mode="overwrite")

    print("STOPPING")
    # Stop Spark session
    spark.stop()

    print("COMPLETED")
