from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime
from ingestion.daily_market_data import run_market_ingestion
from ingestion import config


with DAG(   
    dag_id='daily_market_dag',
    schedule='0 22 * * 1-5',  # 10 PM IST Weekdays [5]
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    ingest_daily = PythonOperator(
        task_id='ingest_daily_market_data',
        python_callable=run_market_ingestion,
        op_kwargs={'tickers': config.TEST_TICKERS} 
    )

    load_market_data_to_snowflake = SQLExecuteQueryOperator(
    task_id="load_market_data_to_snowflake",
    conn_id='snowflake_default',
    sql="EXECUTE TASK LOAD_MARKET_DATA_TASK;"
    )


    run_dbt = BashOperator(
        task_id='dbt_run_multiples',
        bash_command='cd /opt/airflow/dbt && dbt run --select mart_valuation_multiples+'
    )
    
    ingest_daily >> load_market_data_to_snowflake >> run_dbt
