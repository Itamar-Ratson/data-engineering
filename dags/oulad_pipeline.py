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

# PARALLEL AGGREGATION TASKS
def aggregate_mean_scores(**context):
    """Calculate mean scores per student"""
    # Create output directory
    os.makedirs('/opt/airflow/data/aggregated/mean_scores', exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("Mean Scores Aggregation") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    assessments = spark.read.csv("hdfs://namenode:9000/data/oulad/studentAssessment.csv", header=True, inferSchema=True)
    students = spark.read.csv("hdfs://namenode:9000/data/oulad/studentInfo.csv", header=True, inferSchema=True)
    
    assessments.createOrReplaceTempView("assessments")
    students.createOrReplaceTempView("students")
    
    mean_scores = spark.sql("""
        SELECT 
            a.id_student,
            s.code_module,
            s.code_presentation,
            AVG(a.score) as mean_score,
            COUNT(a.id_assessment) as num_assessments
        FROM assessments a
        JOIN students s ON a.id_student = s.id_student
        WHERE a.score IS NOT NULL AND a.score > 0
        GROUP BY a.id_student, s.code_module, s.code_presentation
    """)
    
    mean_scores.createOrReplaceTempView("mean_scores")
    
    # Debug: Show sample results
    print("=== MEAN SCORES RESULTS ===")
    mean_scores.show(5)
    print(f"Total rows: {mean_scores.count()}")
    
    # NEW APPROACH: Collect data and write manually
    output_path = '/opt/airflow/data/aggregated/mean_scores'
    
    try:
        # Collect all data to driver
        collected_data = mean_scores.collect()
        print(f"Collected {len(collected_data)} rows from Spark")
        
        # Write manually using Python CSV
        import csv
        csv_file_path = os.path.join(output_path, 'mean_scores_data.csv')
        
        with open(csv_file_path, 'w', newline='') as csvfile:
            if collected_data:
                # Get column names from first row
                fieldnames = collected_data[0].asDict().keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data rows
                for row in collected_data:
                    writer.writerow(row.asDict())
        
        print(f"Manual CSV write completed to: {csv_file_path}")
        
    except Exception as e:
        print(f"ERROR during manual write: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    spark.stop()

def aggregate_pass_rates(**context):
    """Calculate pass/fail rates by module"""
    # Create output directory
    os.makedirs('/opt/airflow/data/aggregated/pass_rates', exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("Pass Rates Aggregation") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    assessments = spark.read.csv("hdfs://namenode:9000/data/oulad/studentAssessment.csv", header=True, inferSchema=True)
    students = spark.read.csv("hdfs://namenode:9000/data/oulad/studentInfo.csv", header=True, inferSchema=True)
    
    assessments.createOrReplaceTempView("assessments")
    students.createOrReplaceTempView("students")
    
    # Debug: Check data availability
    print("=== DEBUGGING PASS RATES ===")
    spark.sql("SELECT COUNT(*) as total_assessments FROM assessments").show()
    spark.sql("SELECT COUNT(*) as total_students FROM students").show()
    spark.sql("SELECT COUNT(*) as students_with_scores FROM students s JOIN assessments a ON s.id_student = a.id_student WHERE a.score IS NOT NULL").show()
    
    pass_rates = spark.sql("""
        SELECT 
            s.code_module,
            s.code_presentation,
            COUNT(DISTINCT s.id_student) as total_students,
            COUNT(DISTINCT CASE WHEN a.score >= 40 THEN s.id_student END) as passed_students,
            (COUNT(DISTINCT CASE WHEN a.score >= 40 THEN s.id_student END) * 100.0 / 
             COUNT(DISTINCT s.id_student)) as pass_rate
        FROM students s
        LEFT JOIN assessments a ON a.id_student = s.id_student AND a.score IS NOT NULL AND a.score > 0
        GROUP BY s.code_module, s.code_presentation
        HAVING COUNT(DISTINCT s.id_student) > 5
    """)
    
    # Debug: Show sample results
    print("=== PASS RATES RESULTS ===")
    pass_rates.show(5)
    print(f"Total rows: {pass_rates.count()}")
    
    # NEW APPROACH: Collect data and write manually
    output_path = '/opt/airflow/data/aggregated/pass_rates'
    print(f"About to write to: {output_path}")
    print(f"Directory exists: {os.path.exists(output_path)}")
    
    try:
        # Collect all data to driver
        collected_data = pass_rates.collect()
        print(f"Collected {len(collected_data)} rows from Spark")
        
        # Write manually using Python CSV
        import csv
        csv_file_path = os.path.join(output_path, 'pass_rates_data.csv')
        
        with open(csv_file_path, 'w', newline='') as csvfile:
            if collected_data:
                # Get column names from first row
                fieldnames = collected_data[0].asDict().keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data rows
                for row in collected_data:
                    writer.writerow(row.asDict())
        
        print(f"Manual CSV write completed to: {csv_file_path}")
        
        # Verify the file was created
        if os.path.exists(csv_file_path):
            size = os.path.getsize(csv_file_path)
            print(f"File created successfully! Size: {size} bytes")
            
            # Show first few lines of the file
            with open(csv_file_path, 'r') as f:
                content = f.read(200)
                print(f"File content preview: {content}")
        else:
            print("ERROR: File was not created!")
            
        # Also check what files exist in directory
        files = os.listdir(output_path)
        print(f"All files in directory: {files}")
            
    except Exception as e:
        print(f"ERROR during manual write: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    spark.stop()

def aggregate_assessment_difficulty(**context):
    """Calculate assessment difficulty metrics"""
    # Create output directory
    os.makedirs('/opt/airflow/data/aggregated/assessment_difficulty', exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("Assessment Difficulty") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    assessments = spark.read.csv("hdfs://namenode:9000/data/oulad/studentAssessment.csv", header=True, inferSchema=True)
    
    assessments.createOrReplaceTempView("assessments")
    
    # Debug: Check data availability  
    print("=== DEBUGGING ASSESSMENT DIFFICULTY ===")
    spark.sql("SELECT COUNT(*) as total_assessments FROM assessments").show()
    spark.sql("SELECT COUNT(*) as valid_scores FROM assessments WHERE score IS NOT NULL AND score >= 0 AND score <= 100").show()
    spark.sql("SELECT COUNT(DISTINCT id_assessment) as unique_assessments FROM assessments WHERE score IS NOT NULL").show()
    
    difficulty = spark.sql("""
        SELECT 
            id_assessment,
            ROUND(AVG(score), 2) as avg_score,
            ROUND(STDDEV(score), 2) as score_stddev,
            COUNT(*) as num_submissions,
            CASE 
                WHEN AVG(score) < 50 THEN 'Hard'
                WHEN AVG(score) < 70 THEN 'Medium'
                ELSE 'Easy'
            END as difficulty_level
        FROM assessments
        WHERE score IS NOT NULL AND score >= 0 AND score <= 100
        GROUP BY id_assessment
        HAVING COUNT(*) >= 10
    """)
    
    # Debug: Show sample results
    print("=== ASSESSMENT DIFFICULTY RESULTS ===")
    difficulty.show(5)
    print(f"Total rows: {difficulty.count()}")
    
    # NEW APPROACH: Collect data and write manually
    output_path = '/opt/airflow/data/aggregated/assessment_difficulty'
    print(f"About to write to: {output_path}")
    
    try:
        # Collect all data to driver
        collected_data = difficulty.collect()
        print(f"Collected {len(collected_data)} rows from Spark")
        
        # Write manually using Python CSV
        import csv
        csv_file_path = os.path.join(output_path, 'assessment_difficulty_data.csv')
        
        with open(csv_file_path, 'w', newline='') as csvfile:
            if collected_data:
                # Get column names from first row
                fieldnames = collected_data[0].asDict().keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data rows
                for row in collected_data:
                    writer.writerow(row.asDict())
        
        print(f"Manual CSV write completed to: {csv_file_path}")
        
        # Verify the file was created
        if os.path.exists(csv_file_path):
            size = os.path.getsize(csv_file_path)
            print(f"File created successfully! Size: {size} bytes")
        else:
            print("ERROR: File was not created!")
            
    except Exception as e:
        print(f"ERROR during manual write: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    spark.stop()

# PARALLEL LOAD TASKS
def load_mean_scores(**context):
    """Load mean scores to PostgreSQL"""
    conn = psycopg2.connect(
        host="datawarehouse", database="datawarehouse",
        user="datauser", password="datapass", port=5432
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS student_mean_scores (
            id_student INTEGER,
            code_module VARCHAR(10),
            code_presentation VARCHAR(10),
            mean_score FLOAT,
            num_assessments INTEGER
        )
    """)
    
    cur.execute("DELETE FROM student_mean_scores")
    
    import csv
    for file in os.listdir('/opt/airflow/data/aggregated/mean_scores'):
        if file.endswith('.csv'):
            with open(f'/opt/airflow/data/aggregated/mean_scores/{file}', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['id_student'] and row['mean_score']:
                        cur.execute("""
                            INSERT INTO student_mean_scores 
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            int(row['id_student']), row['code_module'],
                            row['code_presentation'], float(row['mean_score']),
                            int(row['num_assessments'])
                        ))
    
    conn.commit()
    cur.close()
    conn.close()

def load_pass_rates(**context):
    """Load pass rates to PostgreSQL"""
    print("=== STARTING LOAD PASS RATES ===")
    
    try:
        print("Attempting to connect to PostgreSQL...")
        conn = psycopg2.connect(
            host="datawarehouse", database="datawarehouse",
            user="datauser", password="datapass", port=5432
        )
        print("PostgreSQL connection successful!")
        
        cur = conn.cursor()
        
        print("Creating table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS module_pass_rates (
                code_module VARCHAR(10),
                code_presentation VARCHAR(10),
                total_students INTEGER,
                passed_students INTEGER,
                pass_rate FLOAT
            )
        """)
        
        print("Clearing existing data...")
        cur.execute("DELETE FROM module_pass_rates")
        
        # Debug: Check what files exist
        import csv
        base_path = '/opt/airflow/data/aggregated/pass_rates'
        print(f"Looking for files in: {base_path}")
        
        if not os.path.exists(base_path):
            print(f"ERROR: Directory {base_path} does not exist!")
            return
            
        files = os.listdir(base_path)
        print(f"Files found: {files}")
        
        csv_files = [f for f in files if f.endswith('.csv')]
        print(f"CSV files: {csv_files}")
        
        if not csv_files:
            print("ERROR: No CSV files found!")
            return
            
        row_count = 0
        for file in csv_files:
            file_path = f'{base_path}/{file}'
            print(f"Processing file: {file_path}")
            
            with open(file_path, 'r') as f:
                content = f.read()
                print(f"File size: {len(content)} characters")
                print(f"First 200 chars: {content[:200]}")
                
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['code_module'] and row['pass_rate']:
                        cur.execute("""
                            INSERT INTO module_pass_rates 
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            row['code_module'], row['code_presentation'],
                            int(row['total_students']), int(row['passed_students']),
                            float(row['pass_rate'])
                        ))
                        row_count += 1
                        
        print(f"Total rows inserted: {row_count}")
        
        conn.commit()
        print("Data committed to database!")
        cur.close()
        conn.close()
        print("Connection closed successfully!")
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        raise  # Re-raise the exception to fail the task

def load_assessment_difficulty(**context):
    """Load assessment difficulty to PostgreSQL"""
    conn = psycopg2.connect(
        host="datawarehouse", database="datawarehouse",
        user="datauser", password="datapass", port=5432
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS assessment_difficulty (
            id_assessment INTEGER,
            avg_score FLOAT,
            score_stddev FLOAT,
            num_submissions INTEGER,
            difficulty_level VARCHAR(10)
        )
    """)
    
    cur.execute("DELETE FROM assessment_difficulty")
    
    import csv
    for file in os.listdir('/opt/airflow/data/aggregated/assessment_difficulty'):
        if file.endswith('.csv'):
            with open(f'/opt/airflow/data/aggregated/assessment_difficulty/{file}', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['id_assessment'] and row['avg_score']:
                        cur.execute("""
                            INSERT INTO assessment_difficulty 
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            int(row['id_assessment']), float(row['avg_score']),
                            float(row['score_stddev']) if row['score_stddev'] else None,
                            int(row['num_submissions']), row['difficulty_level']
                        ))
    
    conn.commit()
    cur.close()
    conn.close()

def verify_all_results(**context):
    """Verify all loaded data"""
    conn = psycopg2.connect(
        host="datawarehouse", database="datawarehouse",
        user="datauser", password="datapass", port=5432
    )
    cur = conn.cursor()
    
    # Check all tables
    tables = ['student_mean_scores', 'module_pass_rates', 'assessment_difficulty']
    
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"✅ {table}: {count} rows")
    
    cur.close()
    conn.close()

# Define DAG with PARALLEL structure
with DAG(
    'oulad_parallel_pipeline',
    default_args=default_args,
    description='OULAD Parallel Data Pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    # Extract task (single)
    extract_task = PythonOperator(
        task_id='extract_and_load_hdfs',
        python_callable=extract_and_load_to_hdfs
    )
    
    # Parallel aggregation tasks
    agg_mean_scores = PythonOperator(
        task_id='aggregate_mean_scores',
        python_callable=aggregate_mean_scores
    )
    
    agg_pass_rates = PythonOperator(
        task_id='aggregate_pass_rates',
        python_callable=aggregate_pass_rates
    )
    
    agg_difficulty = PythonOperator(
        task_id='aggregate_assessment_difficulty',
        python_callable=aggregate_assessment_difficulty
    )
    
    # Parallel load tasks
    load_mean_scores_task = PythonOperator(
        task_id='load_mean_scores',
        python_callable=load_mean_scores
    )
    
    load_pass_rates_task = PythonOperator(
        task_id='load_pass_rates',
        python_callable=load_pass_rates
    )
    
    load_difficulty_task = PythonOperator(
        task_id='load_assessment_difficulty',
        python_callable=load_assessment_difficulty
    )
    
    # Verify task (single)
    verify_task = PythonOperator(
        task_id='verify_all_results',
        python_callable=verify_all_results
    )
    
    # Define PARALLEL dependencies
    extract_task >> [agg_mean_scores, agg_pass_rates, agg_difficulty]
    
    agg_mean_scores >> load_mean_scores_task
    agg_pass_rates >> load_pass_rates_task
    agg_difficulty >> load_difficulty_task
    
    [load_mean_scores_task, load_pass_rates_task, load_difficulty_task] >> verify_task
