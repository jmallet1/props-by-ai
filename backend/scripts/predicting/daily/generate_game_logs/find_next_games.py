from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_date, date_format
from nba_api.stats.endpoints import playernextngames
from scripts.utils.etl import load, create_spark_session, extract
import pandas as pd
import requests
import time


def get_teams(spark) -> list:

    input_query = """
    SELECT abbreviation
    FROM nba.team_lkp
    """

    teams = extract(query=input_query, spark=spark)

    teams_list = [row["abbreviation"] for row in teams.collect()]

    return teams_list

def get_player_ids(team, spark) -> list:

    input_query = f"""
    SELECT player_id
    FROM nba.player_lkp
    WHERE team = '{team}'
    """

    players = extract(query=input_query, spark=spark)

    player_list = [row["player_id"] for row in players.collect()]

    return player_list

def get_next_game(players, team):

    max_retries = 3

    for player in players:
        retries = 0
        game = {}
        try:
            next_game_api_response = playernextngames.PlayerNextNGames(player_id=player, number_of_games=1)
            next_game_df = next_game_api_response.get_data_frames()[0]

            game['team'] = team
            game['game_date'] = next_game_df['GAME_DATE'].iloc[0]
            game['home_team_abbr'] = next_game_df['HOME_TEAM_ABBREVIATION'].iloc[0]
            game['away_team_abbr'] = next_game_df['VISITOR_TEAM_ABBREVIATION'].iloc[0]

            return game
        except requests.exceptions.ReadTimeout:
            if retries is 2:
                print(f"Failed to load player id: {player}\n{e}")
                raise Exception
            
            retries += 1
            print(f"Timeout occurred, retrying ({retries}/{max_retries})...")
            time.sleep(2 ** retries)  # Exponential backoff

        except Exception as e:
            print(f"Failed to load {player}")
            raise Exception

        

def handler():

    spark = create_spark_session()

    output_table = "predicting.next_games"

    teams = get_teams(spark=spark)
    games = []

    for team in teams:

        players = get_player_ids(team=team, spark=spark)
        game = get_next_game(players=players, team=team)
        games.append(game)

    spark_df = spark.createDataFrame(games)

    # Convert to YYYY-MM-DD format
    spark_df = spark_df.withColumn("next_game", date_format(to_date(spark_df["game_date"], "MMM dd, yyyy"), "yyyy-MM-dd"))
    spark_df = spark_df.drop('game_date')

    load(df=spark_df, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()