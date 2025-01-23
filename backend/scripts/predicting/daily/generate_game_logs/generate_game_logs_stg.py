from pyspark.sql import functions as pysp
from nba_api.stats.endpoints import playergamelog
from pyspark.sql.dataframe import DataFrame
from scripts.utils.etl import extract, load, truncate, create_spark_session
import pandas as pd
import requests
import time

def extract_from_api(player_id):
    """Extracts this years game logs for the given player id"""

    max_retries = 3
    retries = 0
    while retries < max_retries:
        try:
            # Retrieve game log dataframe
            api_data = playergamelog.PlayerGameLog(player_id=player_id)
            game_log = api_data.get_data_frames()[0]

            # next_game_api_response = playernextngames.PlayerNextNGames(player_id=player_id, number_of_games=1)
            # next_game_df = next_game_api_response.get_data_frames()[0]

            # game_log['next_game'] = next_game_df['GAME_DATE'].iloc[0]
            # # spark_df = spark_df.withColumn("next_game", pysp.lit(date_of_next_game))

            # game_log['home_team_abbr'] = next_game_df['HOME_TEAM_ABBREVIATION'].iloc[0]
            # # spark_df = spark_df.withColumn("home_team_abbr", pysp.lit(home_team_abbr))

            # game_log['away_team_abbr'] = next_game_df['VISITOR_TEAM_ABBREVIATION'].iloc[0]
            # # spark_df = spark_df.withColumn("away_team_abbr", pysp.lit(away_team_abbr))

            print(f"Loaded player id: {player_id}")

            return game_log
        except requests.exceptions.ReadTimeout:
            if retries is 2:
                print(f"Failed to load player id: {player_id}\n{e}")
                raise Exception
            retries += 1
            print(f"Timeout occurred, retrying ({retries}/{max_retries})...")
            time.sleep(2 ** retries)  # Exponential backoff
        except Exception as e:
            print(f"Failed to load player id: {player_id}\n{str(e)}")
            raise Exception
            break
    
    return None  # Return None if retries failed


def handler():

    spark = create_spark_session()

    input_table = "predicting.todays_players"  # Name of the input table in PostgreSQL
    output_table = "predicting.player_game_logs_stg"  # Name of the output table in PostgreSQL

    truncate(output_table)

    input_query = f"""
    SELECT
        distinct(player_id)
    FROM
        {input_table}
    WHERE
        player_id IS NOT NULL
    """

    player_query_result = extract(query=input_query, spark=spark)

    # Transform query into 1d array of player ids
    player_ids = player_query_result.rdd.map(lambda row: row["player_id"]).collect()
    print(f"Loading {len(player_ids)} players...")

    # Call the NBA API to retrieve stats for each player
    i = 1
    all_players_data = []

    for player in player_ids:
        time.sleep(2)
        if i % 50 is 0:
            print(f"On player {i}")
        data = extract_from_api(player)
        if data is not None:
            all_players_data.append(data)
        i+=1

    # Combine all player data
    if all_players_data:
        combined_df = pd.concat(all_players_data, ignore_index=True)
        player_game_logs = spark.createDataFrame(combined_df)

        load(df=player_game_logs, table=output_table, mode="append")

    spark.stop()