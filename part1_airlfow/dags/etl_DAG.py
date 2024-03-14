import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from steps.messages import send_telegram_success_message, send_telegram_failure_message
from steps.etl import create_table, extract, load, transform

DATASET_TABLE = 'flat_price_predict'

with DAG(
    dag_id=DATASET_TABLE + '_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule='@once',
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message) as dag:
    
    create_step = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        op_kwargs={'table_name': DATASET_TABLE, 'unique_constraint_name': DATASET_TABLE + '_flat_id'})
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(
        task_id='load',
        python_callable=load,
        op_kwargs={'table_name': DATASET_TABLE}
        )

    create_step >> extract_step >> transform_step >> load_step


if __name__ == '__main__':
    print(DATASET_TABLE)