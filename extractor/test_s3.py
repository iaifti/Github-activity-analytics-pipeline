import boto3
import json
from datetime import datetime

# Configuration
S3_BUCKET = "github-pipeline-istiaq-1"  # YOUR bucket name
S3_KEY = "raw/github_events/test/test_file.json"

def upload_to_s3():
    """Test uploading to S3"""
    
    # Create S3 client
    s3 = boto3.client('s3')
    
    # Test data
    test_data = {
        "message": "Hello from Python!",
        "timestamp": datetime.now().isoformat(),
        "test": True
    }
    
    # Upload
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Body=json.dumps(test_data, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Uploaded to s3://{S3_BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Error uploading: {e}")
        return False
    
    return True

def download_from_s3():
    """Test downloading from S3"""
    
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY
        )
        
        # Read the content
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        print(f"✅ Downloaded from S3:")
        print(json.dumps(data, indent=2))
        
    except Exception as e:
        print(f"❌ Error downloading: {e}")
        return False
    
    return True

def list_s3_files():
    """List files in S3 bucket"""
    
    s3 = boto3.client('s3')
    
    try:
        response = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix='raw/github_events/'
        )
        
        if 'Contents' in response:
            print(f"\n📁 Files in s3://{S3_BUCKET}/raw/github_events/:")
            for obj in response['Contents']:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("📁 No files found")
            
    except Exception as e:
        print(f"❌ Error listing: {e}")

if __name__ == "__main__":
    print("🧪 Testing S3 connection...\n")
    
    # Test upload
    if upload_to_s3():
        print()
        
        # Test download
        download_from_s3()
        print()
        
        # Test listing
        list_s3_files()
        
        print("\n✅ All S3 tests passed!")