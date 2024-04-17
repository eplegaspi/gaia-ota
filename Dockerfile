FROM apache/airflow:2.5.1-python3.9 

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

USER root

COPY airflow-start.sh /airflow-start.sh
RUN chmod +x /airflow-start.sh
USER airflow
ENTRYPOINT ["/bin/bash","/airflow-start.sh"]