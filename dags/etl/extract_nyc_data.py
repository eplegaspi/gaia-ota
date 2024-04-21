# default
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# 3rd party imports
import pandas as pd

# local imports
from services.minio_service import connect_to_minio, fetch_file_from_url, store_file_in_minio, download_parquet_from_minio
from services.mysql_service import connect_to_mysql, insert_data_into_mysql


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'catchup': False,
}

with DAG('extract_nyc_data', 
         default_args=default_args, 
         schedule_interval=None) as dag:

    def filter_and_upload_to_minio(**kwargs):
        year = kwargs['dag_run'].conf.get('year') 
        month = kwargs['dag_run'].conf.get('month')
        year = int(year)
        month = int(month)
            
        api_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

        minio_client = connect_to_minio()

        file_stream = fetch_file_from_url(api_url)

        if file_stream:
            store_file_in_minio(
                minio_client,
                "nycdatatrip",
                f"{year}-{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet",
                file_stream,
                file_stream.getbuffer().nbytes
            )
        else:
            print("Failed to fetch the file from the URL.")

    def download_and_insert_to_mysql(**kwargs):
        year = kwargs['dag_run'].conf.get('year') 
        month = kwargs['dag_run'].conf.get('month')
        year = int(year)
        month = int(month)

        minio_client = connect_to_minio()

        download_parquet_from_minio(
            minio_client,
            "nycdatatrip",
            f"{year}-{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet",
            f"{year}-{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"
        )

        df = pd.read_parquet(f"{year}-{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet")
        filtered_df = df[df['passenger_count'] > 0]

        connection = connect_to_mysql()

        if connection:
            batch_size = 100000
            insert_data_into_mysql(connection, filtered_df, batch_size)
            connection.close()

    filter_and_upload_task = PythonOperator(
        task_id='filter_and_upload_to_minio_task',
        python_callable=filter_and_upload_to_minio,
        provide_context=True
    )
    
    download_and_insert_task = PythonOperator(
        task_id='download_and_insert_to_mysql_task',
        python_callable=download_and_insert_to_mysql,
        provide_context=True
    )

    filter_and_upload_task >> download_and_insert_task