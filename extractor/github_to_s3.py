import requests
import boto3
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPOS = ["apache/airflow", "dbt-labs/dbt-core"]
S3_BUCKET = "github-pipeline-istiaq-1"


def fetch_repo_events(repo, token):

    url = f"https://api.github.com/repos/{repo}/events"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    all_events = []
    page = 1

    while page <=3:
        params = {"per_page": 100, "page": page}
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code != 200:
                print(f"⚠️  Error {response.status_code} for {repo}")
                break
            
            events = response.json()
            
            if not events:
                break

            for event in events:
                event['_extracted_at'] = datetime.utcnow.isoformat()
                event['_source_repo'] = repo
            
            all_events.extend(events)
            print(f"  Page {page}: {len(events)} events")
            
            page += 1
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ Error fetching {repo}: {e}")
            break
    
    return all_events
        
        

def save_to_s3(data, bucket, key):
    s3 = boto3.client('s3')
    
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        print(f"Saved to s3://{bucket}/{key}")
        return True
    except Exception as e:
        print(f"S3 upload error: {e}")
        return False

def run_extraction():
    """Main extraction pipeline"""
    timestamp = datetime.utcnow()
    date_path = timestamp.strftime('year=%Y/month=%m/day=%d')
    
    print(f"🚀 Starting extraction at {timestamp}")
    print(f"📅 Date partition: {date_path}\n")
    
    all_events = []
    
    for repo in REPOS:
        print(f"📦 Processing {repo}...")
        events = fetch_repo_events(repo, GITHUB_TOKEN)
        all_events.extend(events)
        print(f"  Total: {len(events)} events\n")
    
    if all_events:
        # Save to S3 with date partitioning
        filename = f"github_events_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        s3_key = f"raw/github_events/{date_path}/{filename}"
        
        success = save_to_s3(all_events, S3_BUCKET, s3_key)
        
        if success:
            print(f"\n📊 Summary:")
            print(f"  Total events: {len(all_events)}")
            print(f"  Repositories: {len(REPOS)}")
            print(f"  S3 location: s3://{S3_BUCKET}/{s3_key}")
        
        return success
    else:
        print("⚠️  No events extracted")
        return False

if __name__ == "__main__":
    success = run_extraction()
    
    if success:
        print("\n✅ Extraction complete!")
    else:
        print("\n❌ Extraction failed")