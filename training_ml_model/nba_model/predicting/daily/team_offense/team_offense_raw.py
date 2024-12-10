from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, round
from nba_model.utils.etl import extract, load

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load team season data to PostgreSQL") \
    .getOrCreate()

input_table_1 = "predicting.team_offense_stg"  # Name of the input table in PostgreSQL
input_table_2 = "nba.team_lkp"
output_table = "predicting.team_offense_raw"  # Name of the output table in PostgreSQL

def transform(df: DataFrame):

    df = df.withColumn("fg2m", round(col("fg2m"), 3))
    df = df.withColumn("fg2a", round(col("fg2a"), 3))
    df = df.withColumn("fg2p", round(col("fg2p"), 3))

    return df


if __name__ == "__main__":

    query = f"""
    SELECT
        A."TEAM_ID" as team_id,
        A."TEAM_NAME" as team_name,
        A."GP" as gp,
        A."FGM" as fgm,
        A."FGA" as fga,
        A."FG_PCT" as fgp,
        A."FG3M" as fg3m,
        A."FG3A" as fg3a,
        A."FG3_PCT" as fg3p,
        (A."FGM" - A."FG3M") as fg2m,
        (A."FGA" - A."FG3A") as fg2a,
        ((A."FGM" - A."FG3M") / (A."FGA" - A."FG3A")) as fg2p,
        A."FTM" as ftm,
        A."FTA" as fta,
        A."FT_PCT" as ftp,
        A."OREB" as oreb,
        A."DREB" as dreb,
        A."REB" as reb,
        A."AST" as ast,
        A."TOV" as tov,
        A."STL" as stl,
        A."BLK" as blk,
        A."PF" as pf,
        A."PTS" as pts,
        B.abbreviation
    FROM
        {input_table_1} A
    LEFT JOIN
        {input_table_2} B ON A."TEAM_ID" = B.id
    """

    data = extract(query=query, spark=spark)
    transformed_data = transform(data)
    load(df=transformed_data, table=output_table, mode="overwrite")

# Stop Spark session
spark.stop()