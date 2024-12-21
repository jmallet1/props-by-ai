from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.static import players
from nba_model.utils.etl import load
import json
import datetime
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load game log data to PostgreSQL") \
    .getOrCreate()

output_table = "nba.player_lkp"

def get_player_ids() -> DataFrame:

    active_players = players.get_active_players()
    ids = [item['id'] for item in active_players]

    return ids

def get_player_info(player_id):

    data_str = commonplayerinfo.CommonPlayerInfo(player_id=player_id).get_json()
    data_json = json.loads(data_str)

    player_row = data_json['resultSets'][0]['rowSet'][0]

    player_info = {
        'player_id': player_row[0],
        'player_name': player_row[3],
        'position': player_row[15],
        'team': player_row[20]
    }

    return player_info

if __name__ == "__main__":

    # Get all active player ids
    player_ids = get_player_ids()

    print(f"Loading {len(player_ids)} players...")

    player_info_list = []

    i = 0

    # Traverse player ids, getting json object with info from api and adding to list
    for player_id in player_ids:

        player_info = get_player_info(player_id=player_id)
        player_info_list.append(player_info)
        
        i+=1
        if(i % 100 == 0):
            print(f"On player {i}. Current time: {datetime.datetime.now()}")
        
        # Wait 3 sec to not overload API
        time.sleep(3)


    # Create a DataFrame from the list
    player_info_df = spark.createDataFrame(player_info_list)
    load(df=player_info_df, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()