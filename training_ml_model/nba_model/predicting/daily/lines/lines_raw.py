from nba_model.utils.etl import extract, load


input_table_1 = "nba.lines_stg"
input_table_2 = "nba.player_lkp"
input_table_3 = "nba.team_lkp"
output_table = "nba.lines_raw"

# Get the most recent line, for each prop, for each player
input_query = f"""
(SELECT *
FROM (
    SELECT
        lines.player_name,
        lines.away_team,
        away_team_lkp.abbreviation as away_team_abbr,
        lines.home_team,
        home_team_lkp.abbreviation as home_team_abbr,
        lines.date::DATE,
        lines.line,
        lines.odds,
        lines.ou,
        lines.sportsbook_name,
        lines.type,
        lkp.id AS player_id,
        MIN(lines.date) OVER (PARTITION BY lines.player_name) AS min_date
    FROM 
        {input_table_1} AS lines
    LEFT JOIN 
        {input_table_2} AS lkp
        ON lines.player_name = lkp.full_name
    LEFT JOIN
        {input_table_3} AS home_team_lkp ON lines.home_team = home_team_lkp.full_name
    LEFT JOIN
        {input_table_3} AS away_team_lkp ON lines.away_team = away_team_lkp.full_name
)
WHERE date = min_date::DATE)
"""

data = extract(input_query)
load(df=data, table=output_table, mode="overwrite")