from training_ml_model.nba_model.utils.etl import extract, load
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, row_number, when, initcap
from pyspark.sql.window import Window


input_table_1 = "nba.lines_stg"
input_table_2 = "nba.player_lkp"
input_table_3 = "nba.team_lkp"
output_table = "predicting.lines_raw"

def transform(df):

    # Group by player, type of prop, and over/under, then order by the highest line, then lowest line. Tie breaker is the odds closest to 1
    over_window = Window.partitionBy("player_id", "type", "ou").orderBy(col("line").asc(), col("abs_diff").asc())
    under_window = Window.partitionBy("player_id", "type", "ou").orderBy(col("line").desc(), col("abs_diff").asc())

    # Add a rank column based on the condition
    df = df.withColumn(
        "rank",
        when(col("ou") == "Over", row_number().over(over_window))
        .when(col("ou") == "Under", row_number().over(under_window))
    )

    df = df.withColumn("sportsbook_name", initcap(df["sportsbook_name"]))

    # Filter rows where rank = 1
    df_min_max = df.filter(col("rank") == 1).drop("rank")

    return df_min_max

def handler():
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Load team season data to PostgreSQL") \
        .getOrCreate()

    # Get the most recent line, for each prop, for each player
    input_query = f"""
    SELECT *
    FROM (
        SELECT
            lines.player_name,
            lines.away_team,
            away_team_lkp.abbreviation as away_team_abbr,
            lines.home_team,
            home_team_lkp.abbreviation as home_team_abbr,
            lines.date::DATE,
            lines.abs_diff,
            lines.line,
            lines.odds,
            lines.ou,
            lines.sportsbook_name,
            lines.type,
            lkp.player_id,
            MIN(lines.date) OVER (PARTITION BY lines.player_name) AS min_date
        FROM 
            {input_table_1} AS lines
        LEFT JOIN 
            {input_table_2} AS lkp
            ON lines.player_name = lkp.player_name
        LEFT JOIN
            {input_table_3} AS home_team_lkp ON lines.home_team = home_team_lkp.full_name
        LEFT JOIN
            {input_table_3} AS away_team_lkp ON lines.away_team = away_team_lkp.full_name
    )
    WHERE 
        date = min_date::DATE 
        AND player_id IS NOT NULL
    """

    data = extract(query=input_query, spark=spark)
    transformed_df = transform(data)
    load(df=transformed_df, table=output_table, mode="overwrite")