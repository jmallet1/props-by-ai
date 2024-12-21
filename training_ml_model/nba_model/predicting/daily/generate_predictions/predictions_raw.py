from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from nba_model.utils.etl import extract, load

#TODO: Get any of the missing columns that are needed in dynamo db, then send to ddb with a new file. Finish up react side, host website and done


output_table = "predicting.predictions_raw"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load team season data to PostgreSQL") \
    .getOrCreate()

def transform(df: DataFrame) -> DataFrame:

    df = df.drop("high_pid", "high_type", "low_pid", "low_type")

    return df

if __name__ == "__main__":

    predictions_stg = "predicting.predictions_stg"
    lines_raw = "predicting.lines_raw"
    player_lkp = "nba.player_lkp"

    predictions_query = f"""
    SELECT 
        predictions.player_id,
        predictions.prediction,
        predictions.matchup,
        predictions.prop_type,
        lkp.team
    FROM 
        {predictions_stg} predictions
    LEFT JOIN {player_lkp} lkp 
        ON predictions.player_id = lkp.player_id
    """

    # Retrieve the high line for each player and type of prop
    high_lines_query = f"""
    SELECT 
        player_id as high_pid,
        player_name,
        date,
        type as high_type,
        line as high_line,
        sportsbook_name as high_sportsbook,
        odds as high_odds
    FROM (
        SELECT
            player_id, 
            player_name,
            date,
            type,
            line,
            sportsbook_name,
            odds,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, type
                ORDER BY line DESC, abs_diff ASC
            ) AS line_rank
        FROM predicting.lines_raw
    )
    WHERE line_rank = 1
    """

    # Retrieve the low line for each player and type of prop
    low_lines_query = f"""
    SELECT 
        player_id as low_pid,
        type as low_type,
        line as low_line,
        sportsbook_name as low_sportsbook,
        odds as low_odds
    FROM (
        SELECT
            player_id, 
            type,
            line,
            sportsbook_name,
            odds,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, type
                ORDER BY line ASC, abs_diff ASC
            ) AS line_rank
        FROM predicting.lines_raw
    )
    WHERE line_rank = 1
    """

    predictions = extract(query=predictions_query, spark=spark)
    high_lines = extract(query=high_lines_query, spark=spark)
    low_lines = extract(query=low_lines_query, spark=spark)

    df = predictions.join(high_lines, (predictions.player_id == high_lines.high_pid) & (predictions.prop_type == high_lines.high_type), "left") \
                    .join(low_lines, (predictions.player_id == low_lines.low_pid) & (predictions.prop_type == low_lines.low_type), "left")

    transformed_df = transform(df)

    load(df=transformed_df, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()