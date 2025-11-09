import urllib.request
import json
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuration
NPS_KEY = os.environ['NPS_KEY']
PROJECT_ID = 'personal-jtf'
DATASET_ID = 'all_data'
BASE_URL = "https://developer.nps.gov/api/v1"

# Set up BigQuery client
credentials_json = json.loads(os.environ['GOOGLE_CREDENTIALS_JSON'])
credentials = service_account.Credentials.from_service_account_info(credentials_json)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Define endpoints and their corresponding table names
ENDPOINTS = {
    'parks': {'path': '/parks', 'table': 'nps_parks'},
    'amenities': {'path': '/amenities', 'table': 'nps_amenities'},
    'amenities_parks': {'path': '/amenities/parksplaces', 'table': 'nps_amenities_parks'},
    'tours': {'path': '/tours', 'table': 'nps_tours'},
    'thingstodo': {'path': '/thingstodo', 'table': 'nps_things_to_do'},
}

def fetch_endpoint_data(endpoint_name, endpoint_path):
    """Fetch all data from an NPS API endpoint with pagination"""
    print(f"\n=== Fetching {endpoint_name} ===")
    
    all_data = []
    start = 0
    limit = 50
    
    while True:
        url = f"{BASE_URL}{endpoint_path}?start={start}&limit={limit}"
        
        req = urllib.request.Request(url, headers={"X-Api-Key": NPS_KEY})
        
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read())
        except Exception as e:
            print(f"Error fetching {endpoint_name}: {e}")
            break
        
        items = data.get('data', [])
        
        if not items:
            break
        
        all_data.extend(items)
        start += limit
        
        print(f"Fetched {len(all_data)} {endpoint_name} so far...")
    
    print(f"Total {endpoint_name}: {len(all_data)}")
    return all_data

def load_to_bigquery(data, table_name):
    """Load JSON data to BigQuery table"""
    if not data:
        print(f"No data to load for {table_name}")
        return
    
    # Add metadata columns
    load_timestamp = datetime.utcnow().isoformat()
    for record in data:
        record['_loaded_at'] = load_timestamp
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    
    # Load data
    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()  # Wait for job to complete
    
    print(f"Loaded {len(data)} rows to {table_id}")

def main():
    """Main execution function"""
    print("Starting NPS data fetch...")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}")
    
    for endpoint_name, endpoint_config in ENDPOINTS.items():
        data = fetch_endpoint_data(endpoint_name, endpoint_config['path'])
        load_to_bigquery(data, endpoint_config['table'])
    
    print("\n=== Data fetch complete ===")

if __name__ == "__main__":
    main()