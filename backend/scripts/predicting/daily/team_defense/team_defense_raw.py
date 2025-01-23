from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, round
from scripts.utils.etl import extract, load, create_spark_session

input_table_1 = "predicting.team_defense_stg"  # Name of the input table in PostgreSQL
input_table_2 = "nba.team_lkp"
output_table = "predicting.team_defense_raw"  # Name of the output table in PostgreSQL

def transform(df: DataFrame):

    df = df.withColumn("fg2m", round(col("fg2m"), 3))
    df = df.withColumn("fg2a", round(col("fg2a"), 3))
    df = df.withColumn("fg2p", round(col("fg2p"), 3))

    return df

def handler():
    
    # Get necessary team defense features
    query = f"""
    SELECT
        "TEAM_ID" as team_id,
        "TEAM_NAME" as team_name,
        "GP" as gp,
        "OPP_FGM" as fgm,
        "OPP_FGA" as fga,
        "OPP_FG_PCT" as fgp,
        "OPP_FG3M" as fg3m,
        "OPP_FG3A" as fg3a,
        "OPP_FG3_PCT" as fg3p,
        ("OPP_FGM" - "OPP_FG3M") as fg2m,
        ("OPP_FGA" - "OPP_FG3A") as fg2a,
        (("OPP_FGM" - "OPP_FG3M") / ("OPP_FGA" - "OPP_FG3A")) as fg2p,
        "OPP_FTM" as ftm,
        "OPP_FTA" as fta,
        "OPP_FT_PCT" as ftp,
        "OPP_OREB" as oreb,
        "OPP_DREB" as dreb,
        "OPP_REB" as reb,
        "OPP_AST" as ast,
        "OPP_TOV" as tov,
        "OPP_STL" as stl,
        "OPP_BLK" as blk,
        "OPP_PF" as pf,
        "OPP_PTS" as pts,
        "OPP_PTS_RANK" as pts_rnk,
        "OPP_REB_RANK" as reb_rnk,
        "OPP_AST_RANK" as ast_rnk,
        "OPP_STL_RANK" as stl_rnk,
        "OPP_BLK_RANK" as blk_rnk,
        "OPP_TOV_RANK" as tov_rnk,
        B.abbreviation
    FROM
        {input_table_1} A
    LEFT JOIN
        {input_table_2} B ON A."TEAM_ID" = B.id
    """

    spark = create_spark_session()

    data = extract(query=query, spark=spark)
    transformed_data = transform(data)
    load(transformed_data, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()