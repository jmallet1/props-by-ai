from training_ml_model.nba_model.utils.etl import extract, load, transform_prediction_stat
from pyspark.sql import SparkSession


input_table_1 = "predicting.player_game_logs_raw"
input_table_2 = "predicting.lines_raw"
input_table_3 = "predicting.team_defense_raw"
input_table_4 = "predicting.team_offense_raw"
output_table = "predicting.player_pts"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load team season data to PostgreSQL") \
    .getOrCreate()

def handler():
    
    # Only get the players who have a prop for the day in this type
    query_player_data = f"""
    SELECT
        player_id,
        player_name,
        game_date,
        team,
        next_matchup as matchup,
        pts,
        min,
        fgm,
        fga,
        fg_pct,
        fg3m,
        fg3a,
        fg3_pct,
        ftm,
        fta,
        ft_pct,
        oreb,
        b2b_flag,
        away_flag
    FROM 
        {input_table_1}
	WHERE player_id IN (
		SELECT DISTINCT player_id
		FROM {input_table_2}
		WHERE type = 'pts'
	)
    """

    query_opposition_data = f"""
    SELECT
        abbreviation as opp_abbreviation,
        fga::double precision AS opp_fga_per_game,
        fgp::double precision AS opp_fg_percent,
        fg3a::double precision AS opp_x3pa_per_game,
        fg3p::double precision AS opp_x3p_percent,
        fg2m::double precision AS opp_x2p_per_game,
        fg2a::double precision AS opp_x2pa_per_game,
        fg2p::double precision AS opp_x2p_percent,
        fta::double precision AS opp_fta_per_game,
        ftp::double precision AS opp_ft_percent,
        oreb::double precision AS opp_orb_per_game,
        ast::double precision AS opp_ast_per_game,
        stl::double precision AS opp_stl_per_game,
        blk::double precision AS opp_blk_per_game,
        tov::double precision AS opp_tov_per_game,
        pts::double precision AS opp_pts_per_game
    FROM 
        {input_table_3}
    """

    query_team_data = f"""
    SELECT
        abbreviation as tm_abbreviation,
        fga::double precision as fga_per_game,
        fg3a::double precision as x3pa_per_game,
        fg2a::double precision as x2pa_per_game,
        fta::double precision as fta_per_game,
        pts::double precision as pts_per_game
    FROM 
        {input_table_4}
    """

    # Extract data from each table
    player_df = extract(query_player_data, spark=spark)
    opposition_df = extract(query_opposition_data, spark=spark)
    team_df = extract(query_team_data, spark=spark)
    
    stats_to_average = ["pts", "min", "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct", "oreb"]
    points_df = transform_prediction_stat(player_df=player_df, opp_df=opposition_df, team_df=team_df, stats_to_average=stats_to_average)

    # TODO find a way to get the most up to date teams of the players. if a player is traded, the next game will be giving stats from his old team

    load(points_df, output_table, "overwrite")