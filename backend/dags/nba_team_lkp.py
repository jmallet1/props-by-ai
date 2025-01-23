from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from scripts.predicting.static.team_lkp import handler as team_lkp

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id="nba_team_lkp",
    default_args=default_args,
    description="Run PySpark ETL to load NBA team info",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    team_lkp = PythonOperator(
        task_id='team_lkp',
        python_callable=team_lkp,
        provide_context=True,
    )

    team_lkp
