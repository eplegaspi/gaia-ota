CREATE SCHEMA IF NOT EXISTS trip_schema;

USE trip_schema;

CREATE TABLE trip_schema.trip_data (
    VendorID INT,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    Passenger_count INT,
    Trip_distance DECIMAL(10, 2),
    PULocationID INT,
    DOLocationID INT,
    RateCodeID INT,
    Store_and_fwd_flag ENUM('Y', 'N'),
    Payment_type INT,
    Fare_amount DECIMAL(10, 2),
    Extra DECIMAL(10, 2),
    MTA_tax DECIMAL(10, 2),
    Improvement_surcharge DECIMAL(10, 2),
    Tip_amount DECIMAL(10, 2),
    Tolls_amount DECIMAL(10, 2),
    Total_amount DECIMAL(10, 2),
    Congestion_Surcharge DECIMAL(10, 2),
    Airport_fee DECIMAL(10, 2)
);