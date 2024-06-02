import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
import openpyxl


def mount_task(ti):
    name_list = ['Alert_Setting', 'Rule_Setting']
    df_list = []
    
# for index, name in enumerate(name_list):
    df = pd.read_excel('/opt/airflow/pbi_cloud_service/Alert & Rule Setting.xlsx', sheet_name='Alert_Setting', engine='openpyxl')
    # df = pd.read_excel('/opt/airflow/pbi_cloud_service/Alert & Rule Setting.xlsx', sheet_name='Rule_Setting', engine='openpyxl')
    # df = pd.read_excel('/opt/airflow/pbi_cloud_service/Alert & Rule Setting.xlsx', sheet_name=['Alert_Setting', 'Rule_Setting'], engine='openpyxl') #會變dict
    # df = pd.read_excel('/opt/airflow/pbi_cloud_service/Alert & Rule Setting.xlsx', sheet_name=name, engine='openpyxl')
    # df = pd.read_excel('/opt/airflow/pbi_cloud_service/testExcel.xlsx', engine='openpyxl')
    print("\ntype df\n", type(df))
    print("\ndf\n", df)
    df = df.to_json() #單張讀就可以!
    # df = json.dumps(df) 配合dict 又會說他是dataFrame
    df_list.append(df)
    print("aaaa")
    ti.xcom_push(key="mykeyyyy", value=df_list)
    
def read_task(ti):
    print("just done.")
    
    json_string = ti.xcom_pull(task_ids='python_task5', key='mykeyyyy')
    # df = pd.read_json(json_string)
    print("type json string: ", type(json_string))
    df_list = [pd.read_json(json_str) for json_str in json_string] 
    print("after read type df_list:", type(df_list))
    print("after read", df_list)
    print("len df_list", len(df_list))
    print("df_list[0]", type(df_list[0]))
    print("df_list[0]['Alert_ID']: ", df_list[0]['Alert_ID'])
    return None


# 定義建立 XLSX 檔案的函數
def create_xlsx_file(file_path: str):
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Sample Sheet"
    sheet["A1"] = "Hello"
    sheet["B1"] = "World"
    workbook.save(file_path)
    print(f"Created XLSX file at {file_path}")

# 定義刪除 XLSX 檔案的函數
def delete_xlsx_file(file_path: str):
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted XLSX file at {file_path}")
    else:
        print(f"File {file_path} does not exist")
    
with DAG(
    dag_id = 'mount_task',
    schedule_interval = None,
    start_date = datetime(2024,5,11),
    catchup = False,
    tags = ['mount test'],
) as dag:
    
    python_task5 = PythonOperator(
        task_id="python_task5",
        python_callable=mount_task
    )
    
    python_task6 = PythonOperator(
        task_id="python_task6",
        python_callable=read_task
    )
    
        # 建立任務
    create_xlsx = PythonOperator(
        task_id="create_xlsx",
        python_callable=create_xlsx_file,
        op_args=["/opt/airflow/mntpath/file.xlsx"],
        dag=dag,
    )

    delete_xlsx = PythonOperator(
        task_id="delete_xlsx",
        python_callable=delete_xlsx_file,
        op_args=["/opt/airflow/mntpath/Alert & Rule Setting.xlsx"],
        dag=dag,
    )
    
    python_task5 >> python_task6 >> create_xlsx >> delete_xlsx