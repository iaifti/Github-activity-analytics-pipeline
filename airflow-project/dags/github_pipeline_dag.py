from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import boto3
import json
from datetime import datetime
import os

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

 # In production, use Airflow Variables
REPOS = ["apache/airflow", "dbt-labs/dbt-core"]
S3_BUCKET = "github-pipeline-istiaq-1"

def extract_github_events(**context):
    print("🚀 Starting GitHub extraction...")

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    all_events = []

    for repo in REPOS:
        print(f"📦 Processing {repo}...")
        url = f"https://api.github.com/repos/{repo}/events"

        response = requests.get(url, headers=headers, timeout=30)
        print(f"Status code: {response.status_code}")
        response.raise_for_status()

        events = response.json()
        print(f"Fetched {len(events)} events for {repo}")

        all_events.extend(events)

    timestamp = datetime.utcnow()
    date_path = timestamp.strftime("year=%Y/month=%m/day=%d")
    s3_key = f"raw/github_events/{date_path}/events.json"

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(all_events)
    )

    print(f"✅ Saved {len(all_events)} events to s3://{S3_BUCKET}/{s3_key}")

    return {
        "event_count": len(all_events),
        "s3_key": s3_key
    }

def validate_extraction(**context):
    """Validate that extraction succeeded"""
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='extract_github_events')
    
    event_count = metadata['event_count']
    
    if event_count == 0:
        raise ValueError("No events extracted!")
    
    print(f"✅ Validation passed: {event_count} events extracted")
    return True

def load_to_snowflake(**context):
    """Load S3 data to Snowflake"""
    import snowflake.connector
    
    # Snowflake connection
    conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse="dbt_wh",
    database="github_analytics",
    schema="raw",
    )

    
    cursor = conn.cursor()
    
    try:
        # Get metadata from previous task
        ti = context['ti']
        metadata = ti.xcom_pull(task_ids='extract_github_events')
        s3_key = metadata['s3_key']
        
        print(f"📥 Loading from S3: {s3_key}")
        
        # Load specific file from S3
        load_query = f"""
        COPY INTO raw_github_events(raw_data, file_name)
        FROM (
            SELECT 
                $1,
                METADATA$FILENAME
            FROM @s3_github_stage
        )
        FILES = ('{s3_key.split('/')[-1]}')
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = 'CONTINUE';
        """
        
        cursor.execute(load_query)
        result = cursor.fetchone()
        
        rows_loaded = result[0] if result else 0
        print(f"✅ Loaded {rows_loaded} rows to Snowflake")
        
        return {'rows_loaded': rows_loaded}
        
    finally:
        cursor.close()
        conn.close()

# Define the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'github_events_pipeline',
    default_args=default_args,
    description='Extract GitHub events to S3',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['github', 'extraction'],
) as dag:
    
    # Task 1: Extract from GitHub
    extract_task = PythonOperator(
        task_id='extract_github_events',
        python_callable=extract_github_events,
        provide_context=True,
    )
    
    # Task 2: Validate extraction
    validate_task = PythonOperator(
        task_id='validate_extraction',
        python_callable=validate_extraction,
        provide_context=True,
    )

    # Task 3: Load to Snowflake
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
    )
    
    # Update dependencies
    extract_task >> validate_task >> load_task
