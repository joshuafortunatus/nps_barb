# NPS Data Pipeline

Automated data pipeline that fetches National Parks Service (NPS) API data nightly and loads it into BigQuery for analysis.

## Overview

This pipeline collects data from the NPS API for all 66 US National Parks and loads it into BigQuery tables. The data includes parks information, amenities, tours, activities, and upcoming events.

## Data Sources

The pipeline fetches data from the following NPS API endpoints:

- **Parks** - Basic park information, locations, descriptions
- **Amenities** - Available amenities across parks
- **Amenities Parks** - Park-specific amenity mappings
- **Tours** - Guided tours available at parks
- **Things to Do** - Activities and attractions
- **Events** - Upcoming events (filtered to end on or after today)

## Infrastructure

- **API**: [National Parks Service API](https://www.nps.gov/subjects/developer/api-documentation.htm)
- **Data Warehouse**: Google BigQuery
- **Orchestration**: GitHub Actions (runs nightly at 2 AM UTC)
- **Language**: Python 3.11

## National Parks Covered

All 66 US National Parks are included:

Acadia, Arches, Badlands, Big Bend, Biscayne, Black Canyon of the Gunnison, Bryce Canyon, Canyonlands, Capitol Reef, Carlsbad Caverns, Channel Islands, Congaree, Crater Lake, Cuyahoga Valley, Denali, Dry Tortugas, Death Valley, Everglades, Gates of the Arctic, Gateway Arch, Glacier, Glacier Bay, Great Basin, Grand Canyon, Great Sand Dunes, Grand Teton, Great Smoky Mountains, Guadalupe Mountains, Haleakalā, Hawaiʻi Volcanoes, Hot Springs, Indiana Dunes, Isle Royale, Joshua Tree, Katmai, Kenai Fjords, Kings Canyon, Kobuk Valley, Lake Clark, Lassen Volcanic, Mammoth Cave, Mesa Verde, Mount Rainier, New River Gorge, National Park of American Samoa, Olympic, Petrified Forest, Pinnacles, Redwood, Rocky Mountain, Saguaro, Sequoia, Shenandoah, Theodore Roosevelt, Virgin Islands, Voyageurs, White Sands, Wind Cave, Wrangell-St. Elias, Yellowstone, Yosemite, Zion

## Setup

### Prerequisites

- Python 3.11+
- Google Cloud Project with BigQuery enabled
- NPS API key (free from [NPS Developer Portal](https://www.nps.gov/subjects/developer/get-started.htm))

### Required Secrets

Configure the following GitHub secrets:

- `NPS_KEY` - Your NPS API key
- `GOOGLE_CREDENTIALS_JSON` - GCP service account JSON credentials
- `PROJECT_ID` - Google Cloud project ID
- `DATASET_ID` - BigQuery dataset ID

### Local Development

1. Clone the repository
```bash
git clone <repo-url>
cd nps-data-pipeline
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Set environment variables
```bash
export NPS_KEY="your-api-key"
export GOOGLE_CREDENTIALS_JSON='{"type": "service_account", ...}'
export PROJECT_ID="your-project-id"
export DATASET_ID="your-dataset-id"
```

4. Run the pipeline
```bash
python main.py
```

## BigQuery Tables

Data is loaded into the following tables in BigQuery:

- `nps_parks` - Park information
- `nps_amenities` - Available amenities
- `nps_amenities_parks` - Park-amenity mappings
- `nps_tours` - Guided tours
- `nps_things_to_do` - Activities
- `nps_events` - Upcoming events

All tables include a `_loaded_at` timestamp column for tracking data freshness.

## Schedule

The pipeline runs automatically every night at 2 AM UTC via GitHub Actions. It can also be triggered manually from the Actions tab in GitHub.

## Data Handling

- **JSON Fields**: Complex nested data (arrays, objects) are stored as JSON strings to prevent unnesting
- **Deduplication**: Events are deduplicated by ID to handle API pagination quirks
- **Write Mode**: Tables are fully refreshed (TRUNCATE) on each run
- **Events Filtering**: Only events ending on or after the current date are loaded

## Error Handling

- API errors are logged and the pipeline continues with remaining endpoints
- Invalid records are skipped with warnings
- Empty datasets are handled gracefully without creating tables

## License

MIT
