from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.predicting.daily.team_defense.team_defense_stg import handler as defense_stg
from scripts.predicting.daily.team_defense.team_defense_raw import handler as defense_raw
from scripts.predicting.daily.team_offense.team_offense_stg import handler as offense_stg
from scripts.predicting.daily.team_offense.team_offense_raw import handler as offense_raw
from scripts.predicting.daily.generate_game_logs.find_todays_players import handler as todays_players
from scripts.predicting.daily.generate_game_logs.find_next_games import handler as next_games
from scripts.predicting.daily.generate_game_logs.generate_game_logs_stg import handler as game_logs_stg
from scripts.predicting.daily.generate_game_logs.generate_game_logs_raw import handler as game_logs_raw
from scripts.predicting.daily.lines.lines_stg import handler as lines_stg
from scripts.predicting.daily.lines.lines_raw import handler as lines_raw
from airflow.utils.task_group import TaskGroup
from scripts.predicting.daily.to_ddb import load_lines, load_nba_player_data, load_player_info, load_team_ranks
from scripts.predicting.daily.generate_predictions import (points_features, rebounds_features, assists_features,
                                                           blocks_features, steals_features, turnovers_features,
                                                           predictions_stg, predictions_raw)


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# Define the DAG
with DAG(
    dag_id="load_data",
    default_args=default_args,
    description="Run PySpark ETL to load updated website data",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    with TaskGroup("data_ingestion", tooltip="Ingesting data from APIs") as data_ingestion:
        
        next_games = PythonOperator(
            task_id='next_games',
            python_callable=next_games,
            provide_context=True,
        )

        todays_players = PythonOperator(
            task_id='todays_players',
            python_callable=todays_players,
            provide_context=True,
        )

        game_logs_stg = PythonOperator(
            task_id='game_logs_stg',
            python_callable=game_logs_stg,
            provide_context=True,
        )

        game_logs_raw = PythonOperator(
            task_id='game_logs_raw',
            python_callable=game_logs_raw,
            provide_context=True,
        )

        offense_stg = PythonOperator(
            task_id='offense_stg',
            python_callable=offense_stg,
            provide_context=True,
        )

        offense_raw = PythonOperator(
            task_id='offense_raw',
            python_callable=offense_raw,
        )

        defense_stg = PythonOperator(
            task_id="defense_stg",
            python_callable=defense_stg,
            provide_context=True,

        )

        defense_raw = PythonOperator(
            task_id='defense_raw',
            python_callable=defense_raw,
            provide_context=True,
        )

        # lines_stg = PythonOperator(
        #     task_id="lines_stg",
        #     python_callable=lines_stg,
        #     provide_context=True,

        # )

        # lines_raw = PythonOperator(
        #     task_id='lines_raw',
        #     python_callable=lines_raw,
        #     provide_context=True,
        # )

        next_games >> todays_players >> game_logs_stg >> game_logs_raw
        offense_stg >> offense_raw
        defense_stg >> defense_raw
        # lines_stg >> lines_raw

    with TaskGroup("stat_feature_extraction", tooltip="Extracting features from the loaded data") as stat_feature_extraction:

        points = PythonOperator(
            task_id='points',
            python_callable=points_features.handler,
            provide_context=True,
        )

        rebounds = PythonOperator(
            task_id='rebounds',
            python_callable=rebounds_features.handler,
            provide_context=True,
        )

        assists = PythonOperator(
            task_id='assists',
            python_callable=assists_features.handler,
            provide_context=True,
        )

        steals = PythonOperator(
            task_id='steals',
            python_callable=steals_features.handler,
            provide_context=True,
        )

        blocks = PythonOperator(
            task_id='blocks',
            python_callable=blocks_features.handler,
            provide_context=True,
        )

        turnovers = PythonOperator(
            task_id='turnovers',
            python_callable=turnovers_features.handler,
            provide_context=True,
        )

        points
        rebounds
        assists
        steals
        blocks
        turnovers

    with TaskGroup("generating_predictions", tooltip="Running features through model") as generating_predictions:

        predictions_stg = PythonOperator(
            task_id="predictions_stg",
            python_callable=predictions_stg.handler,
            provide_context=True,
        )

        predictions_raw = PythonOperator(
            task_id='predictions_raw',
            python_callable=predictions_raw.handler,
            provide_context=True,
        )

        predictions_stg >> predictions_raw

    with TaskGroup("load_ddb", tooltip="Load data into DynamoDB") as load_ddb:

        lines = PythonOperator(
            task_id="lines",
            python_callable=load_lines.handler,
            provide_context=True,
        )

        player_data = PythonOperator(
            task_id="player_data",
            python_callable=load_nba_player_data.handler,
            provide_context=True,
        )

        player_info = PythonOperator(
            task_id="player_info",
            python_callable=load_player_info.handler,
            provide_context=True,
        )

        team_ranks = PythonOperator(
            task_id="team_ranks",
            python_callable=load_team_ranks.handler,
            provide_context=True,
        )

        lines
        player_data
        player_info
        team_ranks

    data_ingestion >> stat_feature_extraction >> generating_predictions >> load_ddb

