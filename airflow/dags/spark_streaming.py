from airflow import DAG
from functools import partial
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from send_email import notify_email
default_args = {
    'owner': 'quanganh',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
    dag_id='spark_submit_dag',
    default_args=default_args,
    description='Submit jobs to Spark cluster',
    start_date=datetime(2024, 11, 17),
    schedule_interval=None
) as dag:
    spark_submit = SparkSubmitOperator(
    task_id='spark_submit_job',
    conn_id='spark-conn',
    py_files = '/opt/airflow/dags/spark/postgres_database.py',
    application='dags/spark/streaming_process.py',
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3',
    # conf={
    #     'spark.pyspark.python': '/data/pyspark_venv/bin/python',
    #     'spark.pyspark.driver.python': 'python'
    # }
)
    send_email = PythonOperator(
        task_id = 'send_message_job',
        python_callable=notify_email,
        trigger_rule='one_failed'
    )
    spark_submit >> send_email
