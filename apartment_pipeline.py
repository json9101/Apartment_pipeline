from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 0,
}
test_dag = DAG(
    'Apartment_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)
# Define the BashOperator task
bash_task = BashOperator(
    task_id='Recieve_seoul_data',
    bash_command="""
        aws s3 cp s3://pd24/apartment_price.csv ~/tmp/
    """,
    dag=test_dag
)
translate = BashOperator(
    task_id='translate_csv',
    bash_command="""
    iconv -f EUC-KR -t UTF-8 ~/tmp/apartment_price.csv > ~/tmp/apart_price_ko.csv
    """,
    dag=test_dag
)
bash_task_1 = BashOperator(
    task_id='local_to_hadoop',
    bash_command="""
        hdfs dfs -put -f ~/tmp/apart_price_ko.csv /pd24/hive/apart_price/apart_price_ko.csv
    """,
    dag=test_dag
)
bash_task_2 = BashOperator(
    task_id='Create_table',
    bash_command="""
         hive -f ~/code/Apartment_pipeline/apart_sale.hql
    """,
    dag=test_dag
)
bash_task_3 = BashOperator(
    task_id='raw_data',
    bash_command="""
         hive -f ~/code/Apartment_pipeline/apart_sale_raw.hql
    """,
    dag=test_dag
)
bash_task_4 = BashOperator(
    task_id='base_data',
    bash_command="""
         hive -f ~/code/Apartment_pipeline/apart_sale_base.hql
    """,
    dag=test_dag
)
bash_task_4 = BashOperator(
    task_id='base_data',
    bash_command="""
         hive -f ~/code/Apartment_pipeline/apart_sale_base.hql
    """,
    dag=test_dag
)
bash_task >> translate >> bash_task_1
bash_task_1 >> bash_task_2
bash_task_2 >> bash_task_3
bash_task_3 >> bash_task_4
