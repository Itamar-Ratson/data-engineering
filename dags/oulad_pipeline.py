from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import zipfile
import os
import requests
from pyspark.sql import SparkSession
import psycopg2
import csv
import time

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
    
    time.sleep(30)  # Wait for Hadoop
    
    # Create HDFS directory
    requests.put('http://namenode:9870/webhdfs/v1/data/oulad?op=MKDIRS&user.name=root', timeout=30)
    
    # Upload CSV files
    for root, dirs, files in os.walk('/opt/airflow/data/extracted'):
        for file in files:
            if file.endswith('.csv'):
                local_path = os.path.join(root, file)
                hdfs_path = f'/data/oulad/{file}'
                
                create_url = f'http://namenode:9870/webhdfs/v1{hdfs_path}?op=CREATE&user.name=root&overwrite=true'
                resp = requests.put(create_url, allow_redirects=False, timeout=30)
                
                if resp.status_code == 307:
                    with open(local_path, 'rb') as f:
                        requests.put(resp.headers['Location'], data=f.read(), timeout=60)

def aggregate_mean_scores(**context):
    """Calculate mean scores per student"""
    os.makedirs('/opt/airflow/data/aggregated/mean_scores', exist_ok=True)
    
    spark = SparkSession.builder.appName("Mean Scores").master("local[*]").config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000").getOrCreate()
    
    assessments = spark.read.csv("hdfs://namenode:9000/data/oulad/studentAssessment.csv", header=True, inferSchema=True)
    students = spark.read.csv("hdfs://namenode:9000/data/oulad/studentInfo.csv", header=True, inferSchema=True)
    
    assessments.createOrReplaceTempView("assessments")
    students.createOrReplaceTempView("students")
    
    mean_scores = spark.sql("""
        SELECT a.id_student, s.code_module, s.code_presentation, AVG(a.score) as mean_score, COUNT(a.id_assessment) as num_assessments
        FROM assessments a JOIN students s ON a.id_student = s.id_student
        WHERE a.score IS NOT NULL AND a.score > 0
        GROUP BY a.id_student, s.code_module, s.code_presentation
    """)
    
    # Write manually
    collected_data = mean_scores.collect()
    with open('/opt/airflow/data/aggregated/mean_scores/mean_scores_data.csv', 'w', newline='') as csvfile:
        if collected_data:
            writer = csv.DictWriter(csvfile, fieldnames=collected_data[0].asDict().keys())
            writer.writeheader()
            for row in collected_data:
                writer.writerow(row.asDict())
    
    spark.stop()

def aggregate_pass_rates(**context):
    """Calculate pass rates by module"""
    os.makedirs('/opt/airflow/data/aggregated/pass_rates', exist_ok=True)
    
    spark = SparkSession.builder.appName("Pass Rates").master("local[*]").config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000").getOrCreate()
    
    assessments = spark.read.csv("hdfs://namenode:9000/data/oulad/studentAssessment.csv", header=True, inferSchema=True)
    students = spark.read.csv("hdfs://namenode:9000/data/oulad/studentInfo.csv", header=True, inferSchema=True)
    
    assessments.createOrReplaceTempView("assessments")
    students.createOrReplaceTempView("students")
    
    pass_rates = spark.sql("""
        SELECT s.code_module, s.code_presentation, COUNT(DISTINCT s.id_student) as total_students,
               COUNT(DISTINCT CASE WHEN a.score >= 40 THEN s.id_student END) as passed_students,
               (COUNT(DISTINCT CASE WHEN a.score >= 40 THEN s.id_student END) * 100.0 / COUNT(DISTINCT s.id_student)) as pass_rate
        FROM students s LEFT JOIN assessments a ON a.id_student = s.id_student AND a.score IS NOT NULL AND a.score > 0
        GROUP BY s.code_module, s.code_presentation HAVING COUNT(DISTINCT s.id_student) > 5
    """)
    
    # Write manually
    collected_data = pass_rates.collect()
    with open('/opt/airflow/data/aggregated/pass_rates/pass_rates_data.csv', 'w', newline='') as csvfile:
        if collected_data:
            writer = csv.DictWriter(csvfile, fieldnames=collected_data[0].asDict().keys())
            writer.writeheader()
            for row in collected_data:
                writer.writerow(row.asDict())
    
    spark.stop()

def aggregate_assessment_difficulty(**context):
    """Calculate assessment difficulty"""
    os.makedirs('/opt/airflow/data/aggregated/assessment_difficulty', exist_ok=True)
    
    spark = SparkSession.builder.appName("Assessment Difficulty").master("local[*]").config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000").getOrCreate()
    
    assessments = spark.read.csv("hdfs://namenode:9000/data/oulad/studentAssessment.csv", header=True, inferSchema=True)
    assessments.createOrReplaceTempView("assessments")
    
    difficulty = spark.sql("""
        SELECT id_assessment, ROUND(AVG(score), 2) as avg_score, ROUND(STDDEV(score), 2) as score_stddev, COUNT(*) as num_submissions,
               CASE WHEN AVG(score) < 50 THEN 'Hard' WHEN AVG(score) < 70 THEN 'Medium' ELSE 'Easy' END as difficulty_level
        FROM assessments WHERE score IS NOT NULL AND score >= 0 AND score <= 100
        GROUP BY id_assessment HAVING COUNT(*) >= 10
    """)
    
    # Write manually
    collected_data = difficulty.collect()
    with open('/opt/airflow/data/aggregated/assessment_difficulty/assessment_difficulty_data.csv', 'w', newline='') as csvfile:
        if collected_data:
            writer = csv.DictWriter(csvfile, fieldnames=collected_data[0].asDict().keys())
            writer.writeheader()
            for row in collected_data:
                writer.writerow(row.asDict())
    
    spark.stop()

def load_mean_scores(**context):
    """Load mean scores to PostgreSQL"""
    conn = psycopg2.connect(host="datawarehouse", database="datawarehouse", user="datauser", password="datapass", port=5432)
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE IF NOT EXISTS student_mean_scores (id_student INTEGER, code_module VARCHAR(10), code_presentation VARCHAR(10), mean_score FLOAT, num_assessments INTEGER)")
    cur.execute("DELETE FROM student_mean_scores")
    
    with open('/opt/airflow/data/aggregated/mean_scores/mean_scores_data.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['id_student'] and row['mean_score']:
                cur.execute("INSERT INTO student_mean_scores VALUES (%s, %s, %s, %s, %s)", 
                           (int(row['id_student']), row['code_module'], row['code_presentation'], float(row['mean_score']), int(row['num_assessments'])))
    
    conn.commit()
    cur.close()
    conn.close()

def load_pass_rates(**context):
    """Load pass rates to PostgreSQL"""
    conn = psycopg2.connect(host="datawarehouse", database="datawarehouse", user="datauser", password="datapass", port=5432)
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE IF NOT EXISTS module_pass_rates (code_module VARCHAR(10), code_presentation VARCHAR(10), total_students INTEGER, passed_students INTEGER, pass_rate FLOAT)")
    cur.execute("DELETE FROM module_pass_rates")
    
    with open('/opt/airflow/data/aggregated/pass_rates/pass_rates_data.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['code_module'] and row['pass_rate']:
                cur.execute("INSERT INTO module_pass_rates VALUES (%s, %s, %s, %s, %s)", 
                           (row['code_module'], row['code_presentation'], int(row['total_students']), int(row['passed_students']), float(row['pass_rate'])))
    
    conn.commit()
    cur.close()
    conn.close()

def load_assessment_difficulty(**context):
    """Load assessment difficulty to PostgreSQL"""
    conn = psycopg2.connect(host="datawarehouse", database="datawarehouse", user="datauser", password="datapass", port=5432)
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE IF NOT EXISTS assessment_difficulty (id_assessment INTEGER, avg_score FLOAT, score_stddev FLOAT, num_submissions INTEGER, difficulty_level VARCHAR(10))")
    cur.execute("DELETE FROM assessment_difficulty")
    
    with open('/opt/airflow/data/aggregated/assessment_difficulty/assessment_difficulty_data.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['id_assessment'] and row['avg_score']:
                cur.execute("INSERT INTO assessment_difficulty VALUES (%s, %s, %s, %s, %s)", 
                           (int(row['id_assessment']), float(row['avg_score']), float(row['score_stddev']) if row['score_stddev'] else None, int(row['num_submissions']), row['difficulty_level']))
    
    conn.commit()
    cur.close()
    conn.close()

def verify_results(**context):
    """Verify all loaded data"""
    conn = psycopg2.connect(host="datawarehouse", database="datawarehouse", user="datauser", password="datapass", port=5432)
    cur = conn.cursor()
    
    for table in ['student_mean_scores', 'module_pass_rates', 'assessment_difficulty']:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"✅ {table}: {count} rows")
    
    cur.close()
    conn.close()

# Define DAG
with DAG(
    'oulad_parallel_pipeline',
    default_args=default_args,
    description='Simple OULAD Parallel Pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    # Extract task
    extract_task = PythonOperator(task_id='extract_and_load_hdfs', python_callable=extract_and_load_to_hdfs)
    
    # Parallel aggregation tasks
    agg_mean_scores = PythonOperator(task_id='aggregate_mean_scores', python_callable=aggregate_mean_scores)
    agg_pass_rates = PythonOperator(task_id='aggregate_pass_rates', python_callable=aggregate_pass_rates)
    agg_difficulty = PythonOperator(task_id='aggregate_assessment_difficulty', python_callable=aggregate_assessment_difficulty)
    
    # Parallel load tasks
    load_mean_scores_task = PythonOperator(task_id='load_mean_scores', python_callable=load_mean_scores)
    load_pass_rates_task = PythonOperator(task_id='load_pass_rates', python_callable=load_pass_rates)
    load_difficulty_task = PythonOperator(task_id='load_assessment_difficulty', python_callable=load_assessment_difficulty)
    
    # Verify task
    verify_task = PythonOperator(task_id='verify_results', python_callable=verify_results)
    
    # Define parallel dependencies
    extract_task >> [agg_mean_scores, agg_pass_rates, agg_difficulty]
    agg_mean_scores >> load_mean_scores_task
    agg_pass_rates >> load_pass_rates_task
    agg_difficulty >> load_difficulty_task
    [load_mean_scores_task, load_pass_rates_task, load_difficulty_task] >> verify_task
