# Analyzing NYC Yellow Taxi Trips

## Project Overview
This project outlines an Extract, Transform, Load (ETL) pipeline for processing NYC Yellow Taxi trip data using Dockerized infrastructure. The pipeline is orchestrated by Apache Airflow, with MySQL for metadata storage and Minio for data storage.

## Infrastructure Overview
We'll utilize Docker Compose to manage our infrastructure, which consists of the following services:

- **Airflow**: A platform to programmatically author, schedule, and monitor workflows. It will orchestrate our ETL process.
  
- **MySQL**: A relational database management system. We'll use it to store metadata related to our ETL process, such as task statuses and execution dates.
  
- **Minio**: An open-source object storage server compatible with Amazon S3 API. It will serve as the destination for our extracted data.

- **Metabase**: Metabase is an open-source business intelligence and analytics tool. It provides a simple interface for users to generate and share insights from their data. Metabase can connect to various data sources, including MySQL and Minio, allowing users to create visualizations, dashboards, and run ad-hoc queries on the data stored in these sources. It will provide a user-friendly interface for data exploration and reporting on the extracted and transformed data from your ETL process.


## Setup Instructions
1. Clone the repository from GitHub:
2. Install Docker and Docker Compose if not already installed on your system.
3. Navigate to the project directory.
5. Build the Docker containers using `docker-compose build`.
6. Start the services with `docker-compose up`.

## Execution Instructions
1. Access the Airflow web interface at `http://localhost:8080` (or the configured port).
2. Log in using the username and password obtained from the Docker logs.
![airflow password](readme_images/airflow-password.png)
5. The `schedule_interval` is set to `None`. Run the Airflow DAG manually, passing the arguments `{"year": "<year>", "month": "<month>"}` to initiate the ETL process for the specified year and month.
6. Monitor the workflow execution and view task statuses in the Airflow UI.
![airflow](readme_images/airflow.png)
![airflow](readme_images/execute_1.png)
![airflow](readme_images/execute_2.png)
7. Access the Minio dashboard at `http://localhost:9001` to verify data storage.
8. Access MySQL using `Username: root, Password: admin` to interact with the db storage.
![mysql](readme_images/mysql.png)


## Visualization
To visualize the processed data, you can execute SQL queries against the MySQL database. For example, to obtain the total amount of taxi trips per day after January 1, 2024, you can use the following SQL query:

```sql
SELECT 
    DATE(tpep_pickup_datetime) AS trip_date,
    SUM(Total_amount) AS total_amount_per_day
FROM 
    trip_schema.trip_data
WHERE 
    DATE(tpep_pickup_datetime) >= '2024-01-01'
GROUP BY 
    DATE(tpep_pickup_datetime);
```
![viz](readme_images/viz.png)

## Insights
Extraction: 
- I visited the provided URL and identified the dataset containing the yellow taxi trip records. When I check the URL, it downloads a parquet file.
- The URL format allows for easy dynamic changes, facilitating dynamic data extraction.
- Utilizing direct download methods, I created a python-script to get the data for the specified month.

Loading and Cleaning: 
- Once the data was obtained, I loaded it first to a storage. In this particular instance, I used Minio as a staging layer.
- Note: For production, I'll highly suggest cloud storage such as google cloud storage or AWS S3
- Once the data is in staging layer, I loaded it to a dataframe and filtered out the trips with zero passengers and then inserted the data to mysql
- Note: For production, I'll highly suggest BigQuery for data warehousing.
- Note: For data processing and data transformation, I'll highly suggest Spark or DBT.

Database Setup: 
- To persist the cleaned data, I opted for a SQL database. 
- However, I encountered challenges when attempting to insert a large volume of data into MySQL. 
- The sheer size of the dataset posed a bottleneck, causing performance issues during insertion.
- To overcome this challenge, I implemented batch insertion techniques. 
- Instead of inserting each row individually, I grouped the data into manageable batches and inserted them into the database in chunks. 
- This significantly improved insertion performance and reduced resource overhead.

Aggregation and Visualization: 
- With the clean data loaded into the database, I performed aggregation to calculate the total amount per day. 
- Using SQL queries, I aggregated the data based on date and computed the total amount for each day. 
- Finally, I visualized the aggregated results using Metabase dashboard.