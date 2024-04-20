# Performing ETL Using Dockerized Infrastructure: Extracting and Loading NYC Yellow Taxi Trip Data to Minio

## Infrastructure Overview
We'll utilize Docker Compose to manage our infrastructure, which consists of the following services:

- **Airflow**: A platform to programmatically author, schedule, and monitor workflows. It will orchestrate our ETL process.
  
- **MySQL**: A relational database management system. We'll use it to store metadata related to our ETL process, such as task statuses and execution dates.
  
- **Minio**: An open-source object storage server compatible with Amazon S3 API. It will serve as the destination for our extracted data.

## ETL Process
Our ETL process involves three main steps:

1. **Extract**: Download one month of yellow taxi trip data from the NYC TLC website.
   
2. **Transform**: Perform any necessary data transformations, such as data cleaning or format conversion.
   
3. **Load**: Upload the transformed data to Minio for storage and future analysis.

## Orchestration with Airflow
Airflow orchestrates our ETL workflow.By default, Airflow retrieves the previous month's data at the beginning of the current month. For example, `schedule_interval="0 0 1 * *"` would trigger the workflow to run at midnight on the first day of each month.

Additionally, Airflow provides flexibility to override this default behavior. You can provide specific configurations in the `dag_run` config to specify the year and month as needed.

## Conclusion
In this guide, we've outlined the setup of an ETL pipeline using Dockerized infrastructure. Airflow orchestrates the ETL workflow, MySQL stores metadata, and Minio serves as the destination for extracted data. By following this approach, you can efficiently extract, transform, and load data from various sources into Minio or any other storage destination of your choice.