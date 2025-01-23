from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as pysp
from pyspark.sql.window import Window
from sqlalchemy import create_engine, text


# Database connection properties
db_url = "jdbc:postgresql://host.docker.internal:5432/postgres"

db_properties = {
    "user": "postgres",
    "password": "football0103J",
    "driver": "org.postgresql.Driver"
}

def extract(query: str, spark) -> DataFrame:
    """Extracts data from a PostgreSQL table."""

    return spark.read.jdbc(url=db_url, table=f"({query})", properties=db_properties)

def load(df: DataFrame, table: str, mode: str):
    """Loads data into a PostgreSQL table."""
    df.write.jdbc(url=db_url, table=table, mode=mode, properties=db_properties)
    

def transform_training_stat(player_df, opp_df, team_df, columns_to_average, target_col):

    # Define a window partitioned by player, season, and team, ordered by game date
    window_spec = Window.partitionBy("player_name", "season_year", "team").orderBy("game_date_parsed")

    for col in columns_to_average:
        player_df = player_df.withColumn(f"{col}_5_game_avg", pysp.avg(col).over(window_spec.rowsBetween(-5, -1)))
        player_df = player_df.withColumn(f"{col}_szn_avg", pysp.avg(col).over(window_spec.rowsBetween(Window.unboundedPreceding, -1)))

        # drop all columns besides the target column
        if col != target_col:
            player_df = player_df.drop(col)

        # Join the DataFrames
    joined_df = player_df.join(opp_df, (player_df.matchup == opp_df.opp_abbreviation) & (player_df.season_year == opp_df.opp_season), "left") \
                    .join(team_df, (player_df.team == team_df.tm_abbreviation) & (player_df.season_year == team_df.tm_season), "left")

    joined_df = joined_df.drop(
        "season_year", 
        "game_date_parsed", 
        "player_name", 
        "team",     
        "matchup",
        "opp_abbreviation",  
        "opp_season",       
        "tm_abbreviation",  
        "tm_season"   
    )

    return joined_df

def transform_prediction_stat(player_df, opp_df, team_df, stats_to_average):

    # define a window partitioned by player, and team, ordered by game date
    window_spec = Window.partitionBy("player_id").orderBy(pysp.desc("game_date"))

    # add a rank column to the DF
    player_df = player_df.withColumn("rank", pysp.row_number().over(window_spec))

    # get only the last 5 games and aggregate for each stat
    last_5_games = player_df.filter(pysp.col("rank") <= 5)
    last_5_aggregations = [
        pysp.avg(stat).alias(f"{stat}_5_game_avg") for stat in stats_to_average
    ]

    # calculate last 5-game averages
    last_5_avg = last_5_games.groupBy("player_id").agg(*last_5_aggregations)

    # aggregate for the entire season now
    season_aggregations = [
        pysp.avg(stat).alias(f"{stat}_szn_avg") for stat in stats_to_average
    ]

    # season_aggregations.append(pysp.max("game_date").alias("recent_game_date"))
    season_aggregations.append(pysp.last("team").alias("team"))
    season_aggregations.append(pysp.last("away_flag").alias("away_flag"))
    season_aggregations.append(pysp.last("b2b_flag").alias("b2b_flag"))
    season_aggregations.append(pysp.last("matchup").alias("matchup"))

    # calculate season averages
    season_avg = player_df.groupBy("player_id").agg(*season_aggregations)

    # join last 5 averages and season averages
    player_df = last_5_avg.join(season_avg, on="player_id", how="right")

    ordered_features = get_ordered_features(stats_to_average)
    player_df = player_df.select(ordered_features)

        # Join the DataFrames
    joined_df = player_df.join(opp_df, (player_df.matchup == opp_df.opp_abbreviation), "left") \
                    .join(team_df, (player_df.team == team_df.tm_abbreviation), "left")

    joined_df = joined_df.drop("team", "tm_abbreviation", "opp_abbreviation")

    return joined_df

def get_ordered_features(stats: str):
    misc_columns = ["player_id", "away_flag", "b2b_flag", "matchup", "team"]

    ordered_features = []

    for col in misc_columns:
        ordered_features.append(col)
    
    for stat in stats:
        ordered_features.append(f"{stat}_5_game_avg")
        ordered_features.append(f"{stat}_szn_avg")

    return ordered_features

def truncate(table: str):
    """Deletes data from player_game_logs_stg table"""

    input_query = f"TRUNCATE TABLE {table}"

    # Create a SQLAlchemy engine without the jdbc: prefix
    engine = create_engine(f"postgresql+psycopg2://{db_properties['user']}:{db_properties['password']}@host.docker.internal:5432/postgres")

    # Execute the truncation
    with engine.connect() as conn:
        conn.execute(text(input_query))
        # conn.commit()  # Commit to ensure truncation happens
        print(f"Truncated table: {table}")