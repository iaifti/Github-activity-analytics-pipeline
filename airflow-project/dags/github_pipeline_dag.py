from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import requests
import boto3
import json
from datetime import datetime
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# Configuration: Repository and storage targets
REPOS = ["apache/airflow", "dbt-labs/dbt-core"]
S3_BUCKET = "github-pipeline-istiaq-1"

def extract_github_events(**context):
    """Extract recent events from configured GitHub repositories using GitHub API."""
    print("🚀 Starting GitHub extraction...")
    GITHUB_TOKEN = Variable.get("GITHUB_TOKEN")

    # Set up API headers with authentication token and JSON content type
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    # Aggregate events from multiple repositories
    all_events = []

    for repo in REPOS:
        print(f"Processing {repo}...")
        # Fetch events endpoint for each configured repository
        url = f"https://api.github.com/repos/{repo}/events"

        response = requests.get(url, headers=headers, timeout=30)
        print(f"Status code: {response.status_code}")
        response.raise_for_status()

        events = response.json()
        print(f"Fetched {len(events)} events for {repo}")

        all_events.extend(events)

    # Build S3 path using partitioned structure for efficient data management
    timestamp = datetime.utcnow()
    date_path = timestamp.strftime("year=%Y/month=%m/day=%d")
    s3_key = f"raw/github_events/{date_path}/events.json"

    # Upload aggregated events to S3 for downstream processing
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(all_events)
    )

    print(f"✅ Saved {len(all_events)} events to s3://{S3_BUCKET}/{s3_key}")

    # Return metadata for task dependencies
    return {
        "event_count": len(all_events),
        "s3_key": s3_key
    }

def validate_extraction(**context):
    """Validate that extraction succeeded"""
    ti = context['ti']
    # Retrieve metadata passed from extract_github_events task
    metadata = ti.xcom_pull(task_ids='extract_github_events')
    
    event_count = metadata['event_count']
    
    # Fail fast if extraction yielded no data
    if event_count == 0:
        raise ValueError("No events extracted!")
    
    print(f"✅ Validation passed: {event_count} events extracted")
    return True

def load_to_snowflake(**context):
    """Load S3 data to Snowflake"""
    import snowflake.connector
    
    SNOWFLAKE_USER = Variable.get("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD = Variable.get("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_ACCOUNT = Variable.get("SNOWFLAKE_ACCOUNT")
    
    
    # Initialize Snowflake connection with credentials from environment variables
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
        # Retrieve S3 file path from extraction task via XCom
        ti = context['ti']
        metadata = ti.xcom_pull(task_ids='extract_github_events')
        s3_key = metadata['s3_key']
        
        print(f"Loading from S3: {s3_key}")
        
        # Build COPY command to load JSON events from S3 stage into Snowflake table
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
        
        # Execute COPY command and retrieve row count
        cursor.execute(load_query)
        result = cursor.fetchone()
        
        # Log success with row count
        rows_loaded = result[0] if result else 0
        print(f"Loaded {rows_loaded} rows to Snowflake")
        
        return {'rows_loaded': rows_loaded}
        
    finally:
        # Always close connections to prevent resource leaks
        cursor.close()
        conn.close()

# Define DAG default configuration for all tasks
default_args = {
    'owner': 'data-engineer',  # Assign ownership for support and accountability
    'depends_on_past': False,  # Allow independent task runs regardless of history
    'email_on_failure': False,  # Disable email alerts (configure in alerting service)
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the main GitHub events extraction and loading pipeline
with DAG(
    'github_events_pipeline',
    default_args=default_args,
    description='Extract GitHub events from public repositories and load to Snowflake via S3',
    schedule_interval='0 */6 * * *',  # Execute every 6 hours (0:00, 6:00, 12:00, 18:00 UTC)
    start_date=days_ago(1),
    catchup=False,  # Skip historical runs; start from current date
    tags=['github', 'extraction'],
) as dag:
    
    # Task 1: Fetch GitHub events from API and store in S3
    extract_task = PythonOperator(
        task_id='extract_github_events',
        python_callable=extract_github_events,
        provide_context=True,
    )
    
    # Task 2: Quality check - verify events were extracted successfully
    validate_task = PythonOperator(
        task_id='validate_extraction',
        python_callable=validate_extraction,
        provide_context=True,
    )

    # Task 3: Bulk load validated events from S3 into Snowflake raw layer
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
    )
    
    # Define execution order: extract -> validate -> load
    extract_task >> validate_task >> load_task