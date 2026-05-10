FROM apache/airflow:2.9.3

COPY requirements.txt .
COPY data/neo.csv .
RUN pip install -r requirements.txt