from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import zipfile
import os
import requests
from pyspark.sql import SparkSession
import psycopg2
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
    
    # Wait for Hadoop to be ready
    time.sleep(30)
    
    # Create directory in HDFS
    try:
        response = requests.put('http://namenode:9870/webhdfs/v1/data/oulad?op=MKDIRS&user.name=root', timeout=30)
        print(f"Directory creation response: {response.status_code}")
    except Exception as e:
        print(f"Directory creation error: {e}")
    
    # Upload CSV files to HDFS
    for root, dirs, files in os.walk('/opt/airflow/data/extracted'):
        for file in files:
            if file.endswith('.csv'):
                local_path = os.path.join(root, file)
                hdfs_path = f'/data/oulad/{file}'
                
                try:
                    # Create file
                    create_url = f'http://namenode:9870/webhdfs/v1{hdfs_path}?op=CREATE&user.name=root&overwrite=true'
                    resp = requests.put(create_url, allow_redirects=False, timeout=30)
                    
                    if resp.status_code == 307:
                        # Follow redirect to upload data
                        upload_url = resp.headers['Location']
                        with open(local_path, 'rb') as f:
                            data = f.read()
                        upload_resp = requests.put(upload_url, data=data, timeout=60)
                        
                        if upload_resp.status_code == 201:
                            print(f"Successfully uploaded {file} to HDFS")
                        else:
                            print(f"Upload failed for {file}: {upload_resp.status_code}")
                    else:
                        print(f"Failed to create {file}: {resp.status_code}")
                        
                except Exception as e:
                    print(f"Error uploading {file}: {str(e)}")

def aggregate_data(**context):
    """Use PySpark to aggregate student data"""
    spark = SparkSession.builder \
        .appName("OULAD Aggregation") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    # Read student assessment data
    assessments = spark.read.csv("hdfs://namenode:9000/data/oulad/studentAssessment.csv", header=True, inferSchema=True)
    students = spark.read.csv("hdfs://namenode:9000/data/oulad/studentInfo.csv", header=True, inferSchema=True)
    
    # Register as temp views for SQL
    assessments.createOrReplaceTempView("assessments")
    students.createOrReplaceTempView("students")
    
    # Debug: Check data quality first
    print("=== DEBUGGING DATA QUALITY ===")
    spark.sql("SELECT COUNT(*) as total_assessments FROM assessments").show()
    spark.sql("SELECT COUNT(*) as null_scores FROM assessments WHERE score IS NULL").show()
    spark.sql("SELECT COUNT(*) as empty_scores FROM assessments WHERE score = ''").show()
    
    # Calculate mean exam scores per student (improved filtering)
    mean_scores = spark.sql("""
        SELECT 
            a.id_student,
            s.code_module,
            s.code_presentation,
            AVG(a.score) as mean_score,
            COUNT(a.id_assessment) as num_assessments
        FROM assessments a
        JOIN students s ON a.id_student = s.id_student
        WHERE a.score IS NOT NULL 
          AND a.score != ''
          AND a.score > 0
        GROUP BY a.id_student, s.code_module, s.code_presentation
        HAVING COUNT(a.id_assessment) > 0
    """)
    
    # Debug: Show sample results
    print("=== SAMPLE AGGREGATED DATA ===")
    mean_scores.show(5)
    
    # Save to local filesystem (easier for final step)
    mean_scores.coalesce(1).write.mode('overwrite').csv('/opt/airflow/data/aggregated/mean_scores', header=True)
    
    print("Data aggregation completed successfully")
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
    
    # Clear existing data
    cur.execute("DELETE FROM student_mean_scores")
    
    # Read aggregated data with error handling
    import csv
    row_count = 0
    skipped_count = 0
    
    for file in os.listdir('/opt/airflow/data/aggregated/mean_scores'):
        if file.endswith('.csv'):
            with open(f'/opt/airflow/data/aggregated/mean_scores/{file}', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Skip rows with empty or invalid data
                    if not row['id_student'] or not row['mean_score'] or row['mean_score'] == '':
                        print(f"Skipping row with empty data: {row}")
                        skipped_count += 1
                        continue
                    
                    try:
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
                        row_count += 1
                    except (ValueError, TypeError) as e:
                        print(f"Skipping invalid row: {row} - Error: {e}")
                        skipped_count += 1
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Data loaded to PostgreSQL: {row_count} rows inserted, {skipped_count} rows skipped")

def verify_results(**context):
    """Verify the loaded data and show results"""
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="datawarehouse",
        database="datawarehouse",
        user="datauser",
        password="datapass",
        port=5432
    )
    cur = conn.cursor()
    
    # Get row count
    cur.execute("SELECT COUNT(*) FROM student_mean_scores")
    total_rows = cur.fetchone()[0]
    print(f"Total rows in student_mean_scores: {total_rows}")
    
    # Get sample data
    cur.execute("SELECT * FROM student_mean_scores LIMIT 5")
    rows = cur.fetchall()
    print("\nSample data:")
    for row in rows:
        print(f"Student {row[0]}: Module {row[1]}, Mean Score: {row[3]:.2f}")
    
    # Get statistics by module
    cur.execute("""
        SELECT 
            code_module,
            COUNT(*) as student_count,
            AVG(mean_score) as avg_mean_score,
            MIN(mean_score) as min_score,
            MAX(mean_score) as max_score
        FROM student_mean_scores 
        GROUP BY code_module 
        ORDER BY student_count DESC
    """)
    
    print("\nStatistics by module:")
    for row in cur.fetchall():
        print(f"Module {row[0]}: {row[1]} students, Avg Score: {row[2]:.2f}")
    
    # Get overall statistics
    cur.execute("""
        SELECT 
            AVG(mean_score) as overall_avg,
            MIN(mean_score) as overall_min,
            MAX(mean_score) as overall_max,
            COUNT(DISTINCT code_module) as num_modules
        FROM student_mean_scores
    """)
    
    overall_stats = cur.fetchone()
    print(f"\nOverall Statistics:")
    print(f"Average Score: {overall_stats[0]:.2f}")
    print(f"Score Range: {overall_stats[1]:.2f} - {overall_stats[2]:.2f}")
    print(f"Number of Modules: {overall_stats[3]}")
    
    cur.close()
    conn.close()
    print(f"\n✅ Pipeline verification complete!")

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
    
    verify_task = PythonOperator(
        task_id='verify_results',
        python_callable=verify_results
    )
    
    extract_task >> aggregate_task >> load_task >> verify_task
