from pyspark.sql.dataframe import DataFrame
from nba_api.stats.endpoints import leaguedashplayerstats
from scripts.utils.etl import load, create_spark_session


def get_player_ids() -> DataFrame:

    stat_response = leaguedashplayerstats.LeagueDashPlayerStats(per_mode_detailed='PerGame', last_n_games='51')
    df = stat_response.get_data_frames()[0]
    filtered_df = df.loc[df['PTS'] > 5, ['PLAYER_ID']]
    
    return filtered_df


def handler():

    spark = create_spark_session()
    output_table = "predicting.todays_players"

    # Get all active player ids
    player_ids = get_player_ids()

    # Create a DataFrame from the list
    player_id_df = spark.createDataFrame(player_ids)
    player_id_df = player_id_df.withColumnRenamed("PLAYER_ID", "player_id")
    load(df=player_id_df, table=output_table, mode="overwrite")

    # Stop Spark session
    spark.stop()