from db_test.validation import Validation
from db_test.file_management import File_Management

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
    dag_id = 'db_test',
    schedule_interval = None,
    start_date = datetime(2024,5,11),
    catchup = False,
    tags = ['db test'],
) as dag:
    vd = Validation()
    fm = File_Management()
    
    task1 = PythonOperator(
        task_id="task1",
        python_callable=vd.write_project_info
    )
    
    task2 = PythonOperator(
        task_id="task2",
        python_callable=fm.read_file
    )

task1 >> task2