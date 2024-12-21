import requests
from pyspark.sql import SparkSession
from nba_model.utils.etl import load
from pyspark.sql.functions import col, abs

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load JSON to Postgres") \
    .getOrCreate()

api_key = "7c62197633aba9836307dceefcca23ba"
output_table = "nba.lines_stg"

master_line_list = []

def api_call(api_url):
    response = requests.get(api_url)

    if response.status_code == 200:
        return response.json()  # Parse JSON response
    else:
        print("Request failed with status:", response.status_code)
        return -1

def get_nba_events(): 

    url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey={api_key}"
    
    event_data = api_call(url)

    events = []

    # set to check for duplicate teams, only skip when BOTH already have a game
    # using a set since it's more efficient to search than array
    teams_added = set()

    for event in event_data:
        home_team = event['home_team']
        away_team = event['away_team']

        # Add all of the teams included in props today
        # Do NOT want teams to have multiple games
        if (home_team not in teams_added or away_team not in teams_added):
            teams_added.add(home_team)
            teams_added.add(away_team)

            events.append(
                {
                    'event_id': event['id'],
                    'home_team': home_team,
                    'away_team': away_team,
                    # get first year, month, day, not timestamp
                    'date': event['commence_time'][:10]
                }
            )

    return events

def get_nba_props(event):

    prop_markets = "player_points,player_rebounds,player_assists,player_blocks,player_steals,player_turnovers"
    url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/{event['event_id']}/odds?apiKey={api_key}&regions=us&markets={prop_markets}&oddsFormat=decimal"
    
    sportsbooks = api_call(url)['bookmakers']

    stat_mapping = {
        "player_points": "pts",
        "player_rebounds": "reb",
        "player_assists": "ast",
        "player_steals": "stl",
        "player_blocks": "blk",
        "player_turnovers": "tov"
    }

    # Parse the JSON object to extract all of the player lines in every game

    for sportsbook in sportsbooks:
        sportsbook_name = sportsbook['key']

        for market in sportsbook['markets']:
            
            type = market['key']
            lines = market['outcomes']

            for line in lines:
                master_line_list.append({
                    'player_name': line['description'],
                    'home_team': event['home_team'],
                    'away_team': event['away_team'],
                    'date': event['date'],
                    'sportsbook_name': sportsbook_name,
                    'type': stat_mapping.get(type, "unknown"),
                    'ou': line['name'],
                    'odds': line['price'],
                    'line': line['point']
                })

    return master_line_list

def load_to_historic():
    return 0

if __name__ == "__main__":

    # Get all of the NBA games the sportsbook has (max 1 per team)
    events = get_nba_events()

    # Get every prop for every player in each game
    for event in events:
        get_nba_props(event)
    
    # convert to spark df
    df = spark.createDataFrame(master_line_list)

    df = df.withColumn("abs_diff", abs(col("odds") - 1))

    load(df=df, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()