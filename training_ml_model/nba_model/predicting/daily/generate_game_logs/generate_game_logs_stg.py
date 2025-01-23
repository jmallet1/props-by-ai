from pyspark.sql import SparkSession
from pyspark.sql import functions as pysp
from nba_api.stats.endpoints import playergamelog
from pyspark.sql.dataframe import DataFrame
from training_ml_model.nba_model.utils.etl import extract, load, truncate

# def get_daily_player_info():
#     """Retrieves players with a sportsbook line for the day"""

#     input_query = f"""
#     SELECT
#         distinct(player_id),
#         date as min_date,
#         home_team_abbr,
#         away_team_abbr,
# 		date_rnk
#     FROM (
#         SELECT 
#             RANK() OVER (PARTITION BY player_id ORDER BY date DESC) AS date_rnk,
#             player_id,
#             date,
#             home_team_abbr,
#             away_team_abbr
#         FROM
#             {input_table}
#     )
#     WHERE
#         date_rnk = 1
#         AND player_id IS NOT NULL
#     """

#     query_result = spark.read.jdbc(url=db_url, table=f"({input_query})", properties=db_properties)

#     # Convert the single column to a list
#     player_ids = query_result.rdd.map(lambda row: [row["player_id"], row["min_date"], row["home_team_abbr"], row["away_team_abbr"]]).collect()

#     return player_ids



def extract_from_api(player, spark):
    """Extracts this years game logs for the given player id"""

    # Retrieve game log dataframe
    api_data = playergamelog.PlayerGameLog(player_id=player[0])
    game_log = api_data.get_data_frames()[0]

    # Convert to PySpark DataFrame
    spark_df = spark.createDataFrame(game_log)


    date_of_game = player[1]
    spark_df = spark_df.withColumn("next_game", pysp.lit(date_of_game))
    
    home_team_abbr = player[2]
    spark_df = spark_df.withColumn("home_team_abbr", pysp.lit(home_team_abbr))

    away_team_abbr = player[3]
    spark_df = spark_df.withColumn("away_team_abbr", pysp.lit(away_team_abbr))

    return spark_df


def handler():
    # Define the path to the JAR file inside the container
    jdbc_jar_path = "/opt/airflow/jars/postgresql-42.7.4.jar"

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Load team season data to PostgreSQL") \
        .config("spark.jars", jdbc_jar_path) \
        .getOrCreate()

    input_table = "predicting.lines_raw"  # Name of the input table in PostgreSQL
    output_table = "predicting.player_game_logs_stg"  # Name of the output table in PostgreSQL

    truncate(output_table)

    input_query = f"""
    SELECT
        distinct(player_id),
        date as min_date,
        home_team_abbr,
        away_team_abbr
    FROM
        {input_table}
    WHERE
        player_id IS NOT NULL
    """

    player_query_result = extract(query=input_query, spark=spark)

    # Store each row as an array
    # EX. [[player_id, min_date, home_team_abbr, away_team_abbr], [...]]
    player_info = player_query_result.rdd.map(lambda row: [row["player_id"], row["min_date"], row["home_team_abbr"], row["away_team_abbr"]]).collect()

    # Call the NBA API to retrieve stats for each player
    for player in player_info:
        data = extract_from_api(player, spark)
        load(df=data, table=output_table, mode="append")

    # Stop Spark session
    spark.stop()