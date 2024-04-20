from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from minio import Minio
import os
import requests
# import mysql.connector
from mysql.connector import connect, Error

import urllib3
from minio import Minio
from io import BytesIO


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'catchup': False,
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
    year = kwargs['dag_run'].conf.get('year') 
    month = kwargs['dag_run'].conf.get('month')
    year = int(year)
    month = int(month)
        
    print(f"Retriving data for {year}-{month}")

     # Set up urllib3
    http = urllib3.PoolManager()

    # Set up MinIO client
    minio_client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="admin123456",
        secure=False  # Change to True if your MinIO server uses HTTPS,
    )

    # Define the API endpoint
    api_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

    try:
        # Make a GET request to the API
        response = http.request('GET', api_url)

        # Check if the request was successful
        if response.status == 200:
            # Get the data from the response
            data = response.data

            # Get the total size of the data
            total_size = len(data)

            # Define chunk size (5 MB)
            chunk_size = 5 * 1024 * 1024  # 5 MB in bytes

            # Calculate the number of chunks
            num_chunks = math.ceil(total_size / chunk_size)

            # Split the data into chunks
            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, total_size)
                chunk_data = data[start_idx:end_idx]

                # Wrap the chunk data in a BytesIO object
                data_stream = BytesIO(chunk_data)

                # Upload the chunk to MinIO
                minio_client.put_object(
                    "nycdatatrip",  # Name of your MinIO bucket
                    f"{year}-{month:02d}/yellow_tripdata_{year}-{month:02d}_part{i+1}.parquet",  # Name of the object in MinIO
                    data_stream,    # Use the BytesIO object as the data source
                    len(chunk_data),  # Provide the length of the data
                    content_type="application/octet-stream"  # Adjust content type as needed
                )

            print("Data uploaded successfully to MinIO.")
        else:
            print("Failed to fetch data from the API:", response.status)

    except Exception as e:
        print("Error:", e)

def download_and_insert_to_mysql(**kwargs):
    year = kwargs['dag_run'].conf.get('year') 
    month = kwargs['dag_run'].conf.get('month')
    year = int(year)
    month = int(month)

    # Download filtered Parquet file from MinIO
    bucket_name = "nycdatatrip"
    object_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    download_path = f"/tmp/{object_name}"
    
    # Set up MinIO client
    minio_client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="admin123456",
        secure=False  # Change to True if your MinIO server uses HTTPS,
    )
    minio_client.fget_object(bucket_name, object_name, download_path)
    print(f"Filtered Parquet file downloaded from MinIO bucket {bucket_name}")Í

    CONFIG = {
        "user": "root",
        "password": "admin",
        "host": "mysql"
    }
    
    # Insert into MySQL
    # Connect to MySQL
    with connect(**CONFIG) as connection:
            with connection.cursor() as cursor:
                # Load data from Parquet file into DataFrame
                

                # Iterate over rows and insert into MySQL
                for _, row in df.iterrows():
                    sql = f"INSERT INTO your_table (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, Passenger_count, Trip_distance, PULocationID, DOLocationID, RateCodeID, Store_and_fwd_flag, Payment_type, Fare_amount, Extra, MTA_tax, Improvement_surcharge, Tip_amount, Tolls_amount, Total_amount, Congestion_Surcharge, Airport_fee) VALUES ({row['VendorID']}, '{row['tpep_pickup_datetime']}', '{row['tpep_dropoff_datetime']}', {row['Passenger_count']}, {row['Trip_distance']}, {row['PULocationID']}, {row['DOLocationID']}, {row['RateCodeID']}, '{row['Store_and_fwd_flag']}', {row['Payment_type']}, {row['Fare_amount']}, {row['Extra']}, {row['MTA_tax']}, {row['Improvement_surcharge']}, {row['Tip_amount']}, {row['Tolls_amount']}, {row['Total_amount']}, {row['Congestion_Surcharge']}, {row['Airport_fee']})"
                    cursor.execute(sql)
                
                print("Data inserted into MySQL successfully")

with DAG('download_upload_to_minio_insert_to_mysql', 
         default_args=default_args, 
         schedule_interval=None) as dag:  # Run the DAG monthly

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

    filter_and_upload_task
    filter_and_upload_task >> download_and_insert_task