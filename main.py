import urllib.request
import json
import os
from datetime import datetime, date
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

# National park codes from your CSV
NATIONAL_PARK_CODES = [
    'acad', 'arch', 'badl', 'bibe', 'bisc', 'blca', 'brca', 'cany', 'care', 'cave',
    'chis', 'cong', 'crla', 'cuva', 'dena', 'drto', 'deva', 'ever', 'gaar', 'glac',
    'glba', 'grba', 'grca', 'grsa', 'grte', 'grsm', 'gumo', 'hale', 'havo', 'hosp',
    'indu', 'isro', 'jotr', 'katm', 'kefj', 'kica', 'kova', 'lacl', 'lavo', 'maca',
    'meve', 'mora', 'neri', 'npsa', 'olym', 'pefo', 'pinn', 'redw', 'romo', 'sagu',
    'seki', 'shen', 'thro', 'voya', 'wica', 'wrst', 'yell', 'yose', 'zion'
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

def filter_events(events):
    """Filter events for national parks with end date today or after"""
    print(f"\n=== Filtering events ===")
    today = date.today().isoformat()
    
    filtered_events = []
    for event in events:
        # Check if event has park code in national parks list
        park_code = event.get('parkCode', '').lower()
        if park_code not in NATIONAL_PARK_CODES:
            continue
        
        # Check end date
        date_end = event.get('dateEnd', '')
        if not date_end:
            continue
        
        # Compare dates (ISO format YYYY-MM-DD sorts correctly as strings)
        if date_end >= today:
            filtered_events.append(event)
    
    print(f"Filtered to {len(filtered_events)} events (from {len(events)} total)")
    return filtered_events

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
        
        # Special handling for events endpoint
        if endpoint_name == 'events':
            data = filter_events(data)
        
        load_to_bigquery(data, endpoint_config['table'])
    
    print("\n=== Data fetch complete ===")

if __name__ == "__main__":
    main()