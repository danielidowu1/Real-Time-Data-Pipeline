FROM apache/airflow:2.6.0-python3.9

USER root
# (only if needed)
# RUN apt-get update && apt-get install -y something

COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt