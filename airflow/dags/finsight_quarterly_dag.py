from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.python import PythonSensor
#from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime
from ingestion.config import TEST_TICKERS
from ingestion.yfinance_ingestion import run_ingestion
from dags.config import DEFAULT_ARGS

with DAG(
    dag_id='finsight_quarterly_dag',
    default_args=DEFAULT_ARGS,
    schedule='0 6 1 2,5,8,11 *', # Runs 4x a year [1]
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # TASK 1: THE INGESTION (Must happen first!)
    ingest_financials = PythonOperator(
        task_id='ingest_yfinance_financials',
        python_callable=run_ingestion,
        op_kwargs={'tickers': TEST_TICKERS}
    )
    
    # TASK 2: THE REFINERY (Spark)
    trigger_pyspark_transform = BashOperator(
    task_id='trigger_pyspark_transform',
    bash_command='python3 /opt/airflow/spark/transform_financials.py',
    )

    # TASK 3: THE LOAD (Snowflake)
    load_silver_to_raw_snowflake = SQLExecuteQueryOperator(
        task_id='load_silver_to_raw_snowflake',
        conn_id='snowflake_default', # This connects to the credentials in Airflow UI
        sql="""
        EXECUTE TASK load_income_statement_task;
        EXECUTE TASK load_balance_sheet_task;
        EXECUTE TASK load_cash_flow_task;
        """
    )

    # TASK 4: THE MODELS (dbt)
    dbt_run_quarterly_models = BashOperator(
    task_id='dbt_run_quarterly_models',
    # We use the full path to the dbt binary
    bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --select staging intermediate'
)

    ingest_financials >> trigger_pyspark_transform >> load_silver_to_raw_snowflake >> dbt_run_quarterly_models