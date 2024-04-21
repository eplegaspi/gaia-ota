from minio import Minio
import io
import urllib3

def connect_to_minio():
    minio_client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="admin123456",
        secure=False
    )
    return minio_client

def fetch_file_from_url(url):
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    if response.status == 200:
        return io.BytesIO(response.data)
    else:
        return None

def store_file_in_minio(minio_client, bucket_name, object_name, file_stream, file_size):
    try:
        minio_client.put_object(bucket_name, object_name, file_stream, file_size)
        print("File stored in MinIO successfully.")
    except Exception as e:
        print(f"Failed to store the file in MinIO: {e}")

def download_parquet_from_minio(minio_client, bucket_name, object_name, file_path):
    minio_client.fget_object(bucket_name, object_name, file_path)