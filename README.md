# NPS Data Collection Pipeline For My Friend Barb

Automated data collection from the National Parks Service API with AI-powered hike difficulty ratings

## What it does
- 🌲 Fetches parks, activities, events, and amenities from NPS API
- 🤖 Rates hiking difficulty using Claude AI
- ☁️ Loads everything to BigQuery
- ⏰ Runs automatically every night via GitHub Actions

## Setup
```bash
pip install -r requirements.txt
```

### Required Secrets (GitHub Actions)
- `NPS_KEY` - Your NPS API key
- `ANTHROPIC_API_KEY` - Your Claude API key  
- `PROJECT_ID` - GCP project ID
- `DATASET_ID` - BigQuery dataset name
- `GOOGLE_CREDENTIALS_JSON` - GCP service account JSON

## Local Testing
```bash
export NPS_KEY="your-key"
export ANTHROPIC_API_KEY="your-key"
export PROJECT_ID="your-project-id"
export DATASET_ID="your-dataset-id"
export GOOGLE_CREDENTIALS_JSON='{"type": "service_account", ...}'

python scripts/fetch_nps_data.py
python scripts/rate_hikes.py
```

## Output Tables
- `nps_parks`
- `nps_things_to_do`
- `nps_activity_difficulty_ratings` ⭐ (AI-generated)
- `nps_events`
- `nps_amenities`
- `nps_amenities_parks`
- `nps_tours`
