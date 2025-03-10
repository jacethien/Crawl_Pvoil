from datetime import datetime, timedelta
from to_posgresql import ket_qua
from oil import output_airflow_2
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'thien_phan',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    default_args=default_args,
    dag_id="ETL_Crawl_Pvoil_Mysql_Postgresql",
    start_date=datetime(2024, 6, 11),
    schedule_interval= None
) as dag:
    task1 = PythonOperator(
        task_id='Use_selenium_crawl_oil_and_insert_mysql',
        python_callable= output_airflow_2
    )


    task2 = PythonOperator(
        task_id='connect_postgresql',
        python_callable=ket_qua
    )
    task1 >>   task2


