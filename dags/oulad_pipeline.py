from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import zipfile
import os
from hdfs3 import HDFileSystem
from pyspark.sql import SparkSession
import psycopg2

default_args = {
    'owner': 'data-engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_and_load_to_hdfs(**context):
    """Extract zip file and load CSV files to HDFS"""
    # Extract zip
    with zipfile.ZipFile('/opt/airflow/data/oulad.zip', 'r') as zip_ref:
        zip_ref.extractall('/opt/airflow/data/extracted')
    
    # Connect to HDFS
    hdfs = HDFileSystem(host='hadoop', port=9000)
    
    # Upload CSV files to HDFS
    for root, dirs, files in os.walk('/opt/airflow/data/extracted'):
        for file in files:
            if file.endswith('.csv'):
                local_path = os.path.join(root, file)
                hdfs_path = f'/data/oulad/{file}'
                hdfs.put(local_path, hdfs_path)
                print(f"Uploaded {file} to HDFS")

def aggregate_data(**context):
    """Use PySpark to aggregate student data"""
    spark = SparkSession.builder \
        .appName("OULAD Aggregation") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop:9000") \
        .getOrCreate()
    
    # Read student assessment data
    assessments = spark.read.csv("hdfs://hadoop:9000/data/oulad/studentAssessment.csv", header=True, inferSchema=True)
    students = spark.read.csv("hdfs://hadoop:9000/data/oulad/studentInfo.csv", header=True, inferSchema=True)
    
    # Register as temp views for SQL
    assessments.createOrReplaceTempView("assessments")
    students.createOrReplaceTempView("students")
    
    # Calculate mean exam scores per student
    mean_scores = spark.sql("""
        SELECT 
            a.id_student,
            s.code_module,
            s.code_presentation,
            AVG(a.score) as mean_score,
            COUNT(a.id_assessment) as num_assessments
        FROM assessments a
        JOIN students s ON a.id_student = s.id_student
        GROUP BY a.id_student, s.code_module, s.code_presentation
    """)
    
    # Save to temporary location
    mean_scores.coalesce(1).write.mode('overwrite').csv('/opt/airflow/data/aggregated/mean_scores', header=True)
    
    spark.stop()

def load_to_sql(**context):
    """Load aggregated data to PostgreSQL"""
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="datawarehouse",
        database="datawarehouse",
        user="datauser",
        password="datapass",
        port=5432
    )
    cur = conn.cursor()
    
    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS student_mean_scores (
            id_student INTEGER,
            code_module VARCHAR(10),
            code_presentation VARCHAR(10),
            mean_score FLOAT,
            num_assessments INTEGER
        )
    """)
    
    # Read aggregated data
    import csv
    for file in os.listdir('/opt/airflow/data/aggregated/mean_scores'):
        if file.endswith('.csv'):
            with open(f'/opt/airflow/data/aggregated/mean_scores/{file}', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cur.execute("""
                        INSERT INTO student_mean_scores 
                        (id_student, code_module, code_presentation, mean_score, num_assessments)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        int(row['id_student']),
                        row['code_module'],
                        row['code_presentation'],
                        float(row['mean_score']),
                        int(row['num_assessments'])
                    ))
    
    conn.commit()
    cur.close()
    conn.close()

# Define DAG
with DAG(
    'oulad_pipeline',
    default_args=default_args,
    description='OULAD Data Pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_and_load_hdfs',
        python_callable=extract_and_load_to_hdfs
    )
    
    aggregate_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data
    )
    
    load_task = PythonOperator(
        task_id='load_to_sql',
        python_callable=load_to_sql
    )
    
    extract_task >> aggregate_task >> load_task
