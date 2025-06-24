FROM apache/airflow:2.8.0

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Install PySpark
USER airflow
RUN pip install pyspark==3.5.0 hdfs3 psycopg2-binary
