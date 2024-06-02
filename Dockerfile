FROM apache/airflow:2.1.3
# RUN mkdir -p /home/airflow/container/path
ADD requirements.txt .
RUN pip install -r requirements.txt
