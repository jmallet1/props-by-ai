from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.predicting.static.player_lkp import handler as player_lkp
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id="nba_player_lkp",
    default_args=default_args,
    description="Run PySpark ETL to load NBA player info",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    player_lkp = PythonOperator(
        task_id='player_lkp',
        python_callable=player_lkp,
        provide_context=True,
    )

    player_lkp
