import urllib.request
import json
import os
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuration
NPS_KEY = os.environ['NPS_KEY']
PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = os.environ['DATASET_ID']
BASE_URL = "https://developer.nps.gov/api/v1"

# Set up BigQuery client
credentials_json = json.loads(os.environ['GOOGLE_CREDENTIALS_JSON'])
credentials = service_account.Credentials.from_service_account_info(credentials_json)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# National park codes - sorted alphabetically
NATIONAL_PARK_CODES = [
    'acad', 'arch', 'badl', 'bibe', 'bisc', 'blca', 'brca', 'cany', 'care', 'cave',
    'chis', 'cong', 'crla', 'cuva', 'dena', 'drto', 'deva', 'ever', 'gaar', 'gate',
    'glac', 'glba', 'grba', 'grca', 'grsa', 'grte', 'grsm', 'gumo', 'hale', 'havo',
    'hosp', 'indu', 'isro', 'jotr', 'katm', 'kefj', 'kica', 'kova', 'lacl', 'lavo',
    'maca', 'meve', 'mora', 'neri', 'npsa', 'olym', 'pefo', 'pinn', 'redw', 'romo',
    'sagu', 'seki', 'shen', 'thro', 'viis', 'voya', 'whsa', 'wica', 'wrst', 'yell',
    'yose', 'zion'
]

# Define endpoints and their corresponding table names
ENDPOINTS = {
    'parks': {'path': '/parks', 'table': 'nps_parks'},
    'amenities': {'path': '/amenities', 'table': 'nps_amenities'},
    'amenities_parks': {'path': '/amenities/parksplaces', 'table': 'nps_amenities_parks'},
    'tours': {'path': '/tours', 'table': 'nps_tours'},
    'thingstodo': {'path': '/thingstodo', 'table': 'nps_things_to_do'},
    'events': {'path': '/events', 'table': 'nps_events'},
}

def fetch_endpoint_data(endpoint_name, endpoint_path):
    """Fetch all data from an NPS API endpoint with pagination"""
    print(f"\n=== Fetching {endpoint_name} ===")
    
    all_data = []
    seen_ids = set()
    start = 0
    limit = 50
    
    # Get today's date for events endpoint
    today = date.today().isoformat()
    
    # Create comma-separated park codes for events endpoint
    park_codes_param = ','.join(NATIONAL_PARK_CODES)
    
    while True:
        # Build URL with special handling for events endpoint
        if endpoint_name == 'events':
            url = f"{BASE_URL}{endpoint_path}?parkCode={park_codes_param}&dateEnd={today}&start={start}&limit={limit}"
        else:
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
        
        # For events endpoint, deduplicate
        if endpoint_name == 'events':
            new_count = 0
            for item in items:
                event_id = item.get('id')
                if event_id not in seen_ids:
                    seen_ids.add(event_id)
                    all_data.append(item)
                    new_count += 1
            
            print(f"Fetched {len(items)} events, {new_count} new | Total unique: {len(all_data)}")
            
            # If no new events, stop
            if new_count == 0:
                break
        else:
            all_data.extend(items)
            print(f"Fetched {len(all_data)} {endpoint_name} so far...")
        
        start += limit
    
    print(f"Total {endpoint_name}: {len(all_data)}")
    return all_data

def load_to_bigquery(data, table_name):
    """Load JSON data to BigQuery table without unnesting arrays"""
    if not data:
        print(f"No data to load for {table_name}")
        return
    
    print(f"Loading {len(data)} items to {table_name}")
    
    # Add metadata column
    load_timestamp = datetime.utcnow().isoformat()
    processed_data = []
    
    for i, record in enumerate(data):
        if not isinstance(record, dict):
            print(f"Warning: Skipping non-dict record at index {i} in {table_name}: {type(record)}")
            continue
        
        # Convert arrays to JSON strings to prevent unnesting
        record_copy = {}
        for key, value in record.items():
            if isinstance(value, (list, dict)):
                # Serialize complex types to JSON string
                record_copy[key] = json.dumps(value)
            else:
                record_copy[key] = value
        
        record_copy['_loaded_at'] = load_timestamp
        processed_data.append(record_copy)
    
    if not processed_data:
        print(f"No valid records to load for {table_name}")
        return
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Configure load job - autodetect will create columns but won't unnest since arrays are strings
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    
    # Load data
    job = client.load_table_from_json(processed_data, table_id, job_config=job_config)
    job.result()
    
    print(f"Loaded {len(processed_data)} rows to {table_id}")

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