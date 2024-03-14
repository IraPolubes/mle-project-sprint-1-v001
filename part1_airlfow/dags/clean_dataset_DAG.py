import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from steps.etl import create_table, load
from steps.clean_etl import extract, transform

CLEAN_DATASET_TABLE = 'clean_flat_price_predict'


with DAG(
    dag_id=CLEAN_DATASET_TABLE + '_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule='@once',
) as dag:

    create_step = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        op_kwargs={'table_name': CLEAN_DATASET_TABLE, 'unique_constraint_name': CLEAN_DATASET_TABLE + '_flat_id'})
    extract_step = PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_kwargs={'table_name': CLEAN_DATASET_TABLE}
        )
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(
        task_id='load',
        python_callable=load,
        op_kwargs={'table_name': CLEAN_DATASET_TABLE}
        )

    create_step >> extract_step >> transform_step >> load_step

