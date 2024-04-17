#!/bin/bash
airflow standalone

airflow db migrate

airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@test.org

airflow webserver --port 8080

airflow scheduler