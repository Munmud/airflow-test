from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pipeline import (ingest_data, preprocess_data, train_model,
                     evaluate_model, deploy_model)

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2024, 10, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ml_pipeline',
    default_args=default_args,
    description='ML pipeline using Airflow',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag,
)

t4 = PythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model,
    dag=dag,
)

t5 = PythonOperator(
    task_id='deploy_model',
    python_callable=deploy_model,
    dag=dag,
)

t1 >> t2 >> t3 >> t4 >> t5