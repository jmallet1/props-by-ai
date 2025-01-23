from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, expr, when, datediff
from training_ml_model.nba_model.utils.etl import extract, load


def transform(df: DataFrame):

    df = df.withColumn("team", expr("substr(matchup, 1, 3)"))
    df = df.withColumn("matchup", expr("substr(matchup, -3, 3)"))

    df = df.withColumn(
        "next_matchup",
        when(col("home_team_abbr") == col("team"), col("away_team_abbr"))
        .otherwise(col("home_team_abbr"))
    )

    df = df.withColumn(
        "away_flag",
        when(col("home_team_abbr") == col("team"), 0)
        .otherwise(1)
    )

    df = df.drop("home_team_abbr", "away_team_abbr")

    # Calculate b2b_flag by checking the difference in days between consecutive games
    df = df.withColumn(
        "b2b_flag",
        when(datediff(col("next_game"), col("game_date")) == 1, 1).otherwise(0)
    )

    return df


def handler():

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Load team season data to PostgreSQL") \
        .getOrCreate()

    input_table_1 = "predicting.player_game_logs_stg"  # Name of the input table in PostgreSQL
    input_table_2 = "nba.player_lkp"
    output_table = "predicting.player_game_logs_raw"  # Name of the output table in PostgreSQL

    query = f"""
    SELECT
        A."Player_ID" as player_id,
        A."Game_ID" as game_id,
        TO_DATE(A."GAME_DATE", 'MON DD, YYYY') AS game_date,
        A."MATCHUP" as matchup,
        A."WL" as wl,
        A."MIN" as min,
        A."FGM" as fgm,
        A."FGA" as fga,
        A."FG_PCT" as fg_pct,
        A."FG3M" as fg3m,
        A."FG3A" as fg3a,
        A."FG3_PCT" as fg3_pct,
        A."FTM" as ftm,
        A."FTA" as fta,
        A."FT_PCT" as ft_pct,
        A."OREB" as oreb,
        A."DREB" as dreb,
        A."REB" as reb,
        A."AST" as ast,
        A."STL" as stl,
        A."BLK" as blk,
        A."TOV" as tov,
        A."PF" as pf,
        A."PTS" as pts,
        A."PLUS_MINUS" as plus_minus,
        A.next_game,
        A.home_team_abbr,
        A.away_team_abbr,
        B."full_name" as player_name
    FROM
        {input_table_1} A
    LEFT JOIN 
        {input_table_2} B ON B."id" = A."Player_ID"
    """

    data = extract(query, spark)
    transformed_data = transform(data)
    load(df=transformed_data, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()