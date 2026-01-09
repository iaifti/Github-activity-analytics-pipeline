import requests
import json
import time
from datetime import datetime
from validation import validate_events
import os
from dotenv import load_dotenv

load_dotenv()


GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = "apache/airflow"

def fetch_all_events(per_page=100, max_retries = 3):

    url = f"https://api.github.com/repos/{REPO}/events"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    all_events = []
    page = 1
    
    while True:
        print(f"Fetching page {page}...")
        
        params = {"per_page": per_page, "page": page}
        
        # Try multiple times if it fails
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url, 
                    headers=headers, 
                    params=params,
                    timeout=30  # Fail if takes longer than 30 seconds
                )
                
                # Check for rate limit
                if response.status_code == 403:
                    rate_limit = response.headers.get('X-RateLimit-Remaining', '?')
                    if rate_limit == '0':
                        reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                        wait_seconds = max(reset_time - int(time.time()), 0) + 10
                        print(f"Rate limited. Waiting {wait_seconds} seconds...")
                        time.sleep(wait_seconds)
                        continue 
                
                # Check for other errors
                if response.status_code != 200:
                    print(f"HTTP {response.status_code} on attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                        continue
                    else:
                        print(f"Failed after {max_retries} attempts")
                        return all_events  # Return what we have so far
                
                # Success!
                events = response.json()
                
                if not events:
                    print(f"No more events. Stopped at page {page}")
                    return all_events
                
                print(f"   Got {len(events)} events")
                all_events.extend(events)
                
                break  # Exit retry loop, move to next page
                
            except requests.exceptions.Timeout:
                print(f"Timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    print(f"❌ Timed out after {max_retries} attempts")
                    return all_events
                    
            except requests.exceptions.RequestException as e:
                print(f"Network error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    print(f"Network error after {max_retries} attempts")
                    return all_events
        
        page += 1
        time.sleep(1) 
    
        return all_events

if __name__ == "__main__":
    print(f"Starting extraction at {datetime.now()}")
    
    events = fetch_all_events()
    
    print(f"\nTotal events fetched: {len(events)}")
    
    if events:
        validated_events = validate_events(events)
        
        # Save only validated events
        with open('github_events.json', 'w') as f:
            json.dump(validated_events, f, indent=2)
        print(f"Saved {len(validated_events)} validated events")
    
    print(f"Finished at {datetime.now()}")