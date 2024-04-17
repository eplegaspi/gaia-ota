from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from minio import Minio
import os
import requests
# import mysql.connector



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def download_parquet(year, month):
    # Construct the URL based on the year, month, and trip type
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if request was successful
    if response.status_code == 200:
        # Extract filename from URL
        filename = url.split('/')[-1]
        
        # Define download path
        save_path = os.path.join(os.getcwd(), filename)
        
        # Write the content to a file
        with open(save_path, 'wb') as f:
            f.write(response.content)
        print("Parquet file downloaded successfully.")
        return save_path
    else:
        print("Failed to download Parquet file. Status code:", response.status_code)
        return None

def filter_and_upload_to_minio(**kwargs):
    year = kwargs['dag_run'].conf.get('year', (datetime.now() - timedelta(1)).strftime('%Y'))
    month = kwargs['dag_run'].conf.get('month', (datetime.now() - timedelta(1)).strftime('%m'))
    trip_type = kwargs['dag_run'].conf.get('trip_type', 'yellow')
    
    filename = download_parquet(year, int(month), trip_type)
    
    if filename:
        # Read Parquet file into DataFrame
        df = pd.read_parquet(filename)
        
        # Filter rows where passenger_count > 0
        df_filtered = df[df['passenger_count'] > 0]
        
        # Define MinIO parameters
        minio_host = "http://minio:9000"
        minio_user_root = "minioaccesskey"
        minio_root_password = "miniosecretkey"
        bucket_name = "nycdatatrip"
        object_name = f"yellow_tripdata_filtered_{year}-{month:02d}.parquet"
        
        # Upload filtered DataFrame to MinIO
        client = Minio(minio_host, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)
        client.put_object(bucket_name, object_name, df_filtered.to_parquet())
        print(f"Filtered Parquet file uploaded to MinIO bucket {bucket_name} as {object_name}")

def download_and_insert_to_mysql(**kwargs):
    year = kwargs['dag_run'].conf.get('year', (datetime.now() - timedelta(1)).strftime('%Y'))
    month = kwargs['dag_run'].conf.get('month', (datetime.now() - timedelta(1)).strftime('%m'))
    trip_type = kwargs['dag_run'].conf.get('trip_type', 'yellow')
    
    # Download filtered Parquet file from MinIO
    minio_host = "http://minio:9000"
    minio_access_key = "minioaccesskey"
    minio_secret_key = "miniosecretkey"
    bucket_name = "nycdatatrip"
    object_name = f"{trip_type}_tripdata_filtered_{year}-{month:02d}.parquet"
    download_path = f"/tmp/{object_name}"
    
    client = Minio(minio_host, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)
    client.fget_object(bucket_name, object_name, download_path)
    print(f"Filtered Parquet file downloaded from MinIO bucket {bucket_name}")
    
    # # Insert into MySQL
    # # Connect to MySQL
    # connection = mysql.connector.connect(
    #     host='mysql',
    #     user='airflow',
    #     password='airflow',
    #     database='your_database'
    # )
    
    # cursor = connection.cursor()
    
    # # Load data from Parquet file into DataFrame
    # df = pd.read_parquet(download_path)
    
    # # Iterate over rows and insert into MySQL
    # for index, row in df.iterrows():
    #     sql = "INSERT INTO your_table (col1, col2, col3) VALUES (%s, %s, %s)"
    #     values = (row['col1'], row['col2'], row['col3'])
    #     cursor.execute(sql, values)
    
    # # Commit changes and close connection
    # connection.commit()
    # connection.close()
    # print("Data inserted into MySQL successfully")

with DAG('download_upload_to_minio_insert_to_mysql', 
         default_args=default_args, 
         schedule_interval="0 0 1 * *") as dag:  # Run the DAG monthly

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