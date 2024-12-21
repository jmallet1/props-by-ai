from nba_model.utils.etl import extract, load, transform_prediction_stat
from pyspark.sql import SparkSession


input_table_1 = "predicting.player_game_logs_raw"
input_table_2 = "predicting.lines_raw"
input_table_3 = "predicting.team_defense_raw"
input_table_4 = "predicting.team_offense_raw"
output_table = "predicting.player_blk"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load team season data to PostgreSQL") \
    .getOrCreate()

if __name__ == "__main__":

    # Only get the players who have a prop for the day in this type
    query_player_data = f"""
    SELECT
        player_id,
        player_name,
        game_date,
        team,
        next_matchup as matchup,
        min,
        stl,
        reb,
        blk,
        pf,
        b2b_flag,
        away_flag
    FROM 
        {input_table_1}
	WHERE player_id IN (
		SELECT DISTINCT player_id
		FROM {input_table_2}
		WHERE type = 'blk'
	)
    """

    query_opposition_data = f"""
    SELECT
        abbreviation as opp_abbreviation,
        fgm::double precision AS opp_fg_per_game,
        fga::double precision AS opp_fga_per_game,
        fg3m::double precision AS opp_x3p_per_game,
        fg3a::double precision AS opp_x3pa_per_game,
        fg2m::double precision AS opp_x2p_per_game,
        fg2a::double precision AS opp_x2pa_per_game,
        fta::double precision AS opp_fta_per_game,
        oreb::double precision AS opp_orb_per_game,
        reb::double precision AS opp_trb_per_game,
        tov::double precision AS opp_tov_per_game,
        pts::double precision AS opp_pts_per_game
    FROM 
        {input_table_3}
    """

    query_team_data = f"""
    SELECT
        abbreviation as tm_abbreviation,
        fgm::double precision AS fg_per_game,
        fga::double precision AS fga_per_game,
        pts::double precision as pts_per_game,
        reb::double precision AS trb_per_game,
        blk::double precision AS blk_per_game,
        stl::double precision AS stl_per_game
    FROM 
        {input_table_4}
    """

    # Extract data from each table
    player_df = extract(query_player_data, spark=spark)
    opposition_df = extract(query_opposition_data, spark=spark)
    team_df = extract(query_team_data, spark=spark)
    
    stats_to_average = ["min", "stl", "reb", "blk", "pf"]
    points_df = transform_prediction_stat(player_df=player_df, opp_df=opposition_df, team_df=team_df, stats_to_average=stats_to_average)

    # TODO find a way to get the most up to date teams of the players. if a player is traded, the next game will be giving stats from his old team

    load(points_df, output_table, "overwrite")