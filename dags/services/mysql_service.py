from mysql.connector import connect, Error
import pandas as pd

def connect_to_mysql():
    CONFIG = {
        "user": "root",
        "password": "admin",
        "host": "mysql"
    }
    try:
        connection = connect(**CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def batch_generator(df, batch_size):
    num_batches = len(df) // batch_size + 1
    total_batches = len(df) // batch_size + 1
    print("Total number of batches:", total_batches)
    for i in range(num_batches):
        yield df.iloc[i * batch_size: (i + 1) * batch_size], i + 1, total_batches

def insert_data_into_mysql(connection, df, batch_size):
    with connection.cursor() as cursor:
        for batch_df, batch_num, total_batches in batch_generator(df, batch_size):
            batch_data = []
            for index, row in batch_df.iterrows():
                row_data = (
                    row.get('VendorID', None), row.get('tpep_pickup_datetime', None), row.get('tpep_dropoff_datetime', None),
                    row.get('passenger_count', None), row.get('trip_distance', None), row.get('PULocationID', None),
                    row.get('DOLocationID', None), row.get('RatecodeID', None), row.get('store_and_fwd_flag', None),
                    row.get('payment_type', None), row.get('fare_amount', None), row.get('extra', None), row.get('mta_tax', None),
                    row.get('improvement_surcharge', None), row.get('tip_amount', None), row.get('tolls_amount', None),
                    row.get('total_amount', None), row.get('congestion_surcharge', None), row.get('Airport_fee', None)
                )
                row_data = tuple(None if pd.isna(value) else value for value in row_data)
                batch_data.append(row_data)

            stmt = """
                INSERT INTO trip_schema.trip_data
                (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, Passenger_count, Trip_distance, PULocationID,
                DOLocationID, RateCodeID, Store_and_fwd_flag, Payment_type, Fare_amount, Extra, MTA_tax, Improvement_surcharge,
                Tip_amount, Tolls_amount, Total_amount, Congestion_Surcharge, Airport_fee)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(stmt, batch_data)
            connection.commit()
            print(f"Inserted batch {batch_num}/{total_batches} into the database.")