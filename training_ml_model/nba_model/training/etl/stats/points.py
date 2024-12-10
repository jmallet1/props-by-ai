from nba_model.utils.etl import extract, transform_training_stat, load

min_year = 2013

input_table_1 = "nba.player_game_off_raw"
input_table_2 = "nba.team_season_def_raw"
input_table_3 = "nba.team_season_off_raw"
output_table = "training.player_pts"

if __name__ == "__main__":

    query_player_data = f"""
    SELECT
        player_name,
        matchup,
        season_year,
        game_date_parsed,
        team,
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
        away_flag,
        b2b_flag
    FROM 
        {input_table_1}
    WHERE
        season_year > {min_year}
    """

    query_opposition_data = f"""
    SELECT
        abbreviation as opp_abbreviation,
        opp_fga_per_game::double precision,
        opp_fg_percent::double precision,
        opp_x3pa_per_game::double precision,
        opp_x3p_percent::double precision,
        opp_x2p_per_game::double precision,
        opp_x2pa_per_game::double precision,
        opp_x2p_percent::double precision,
        opp_fta_per_game::double precision,
        opp_ft_percent::double precision,
        opp_orb_per_game::double precision,
        opp_ast_per_game::double precision,
        opp_stl_per_game::double precision,
        opp_blk_per_game::double precision,
        opp_tov_per_game::double precision,
        opp_pts_per_game::double precision,
        season as opp_season
    FROM 
        {input_table_2}
    WHERE
        season > {min_year}
    """

    query_team_data = f"""
    SELECT
        abbreviation as tm_abbreviation,
        fga_per_game::double precision,
        x3pa_per_game::double precision,
        x2pa_per_game::double precision,
        fta_per_game::double precision,
        pts_per_game::double precision,
        season as tm_season
    FROM 
        {input_table_3}
    WHERE
        season > {min_year}
    """

    # Extract data from each table
    player_df = extract(query_player_data)
    opp_df = extract(query_opposition_data)
    team_df = extract(query_team_data)
    
    columns_to_average = ["pts", "min", "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct", "oreb"]
    data_transformed = transform_training_stat(player_df=player_df, opp_df=opp_df, team_df=team_df, columns_to_average=columns_to_average, target_col="pts")

    load(data_transformed, output_table, "overwrite")