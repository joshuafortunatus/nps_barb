#!/usr/bin/env python3
"""
Trail Scraper for NPS Barb Project
Scrapes hiking trail information from NPS websites and loads to BigQuery.
Complements the existing NPS API data collection.
"""

import csv
import json
import os
import re
import time
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Trail:
    """Data structure for trail information"""
    park_code: str
    park_name: str
    trail_name: str
    description: str
    distance: Optional[str] = None
    duration: Optional[str] = None
    difficulty: Optional[str] = None
    elevation_gain: Optional[str] = None
    trail_type: Optional[str] = None
    source_url: str = ""
    scraped_at: str = ""
    
    def to_dict(self):
        return asdict(self)
    
    def to_bigquery_row(self):
        """Convert to BigQuery-compatible format"""
        return {
            'park_code': self.park_code,
            'park_name': self.park_name,
            'trail_name': self.trail_name,
            'description': self.description,
            'distance': self.distance,
            'duration': self.duration,
            'difficulty': self.difficulty,
            'elevation_gain': self.elevation_gain,
            'trail_type': self.trail_type,
            'source_url': self.source_url,
            'scraped_at': self.scraped_at
        }


class NPSTrailScraper:
    """Scraper for NPS trail and hiking information"""
    
    def __init__(self, delay: float = 1.5):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
    def scrape_parks_from_bigquery(self, project_id: str, dataset_id: str) -> List[Trail]:
        """Fetch parks from BigQuery and scrape trail data"""
        # Set up BigQuery client
        credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
        if credentials_json:
            credentials_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            client = bigquery.Client(project=project_id)
        
        # Query parks from BigQuery
        query = f"""
        SELECT park_code, park_full_name, park_url
        FROM `{project_id}.{dataset_id}.nps_parks`
        WHERE park_url IS NOT NULL
        """
        
        logger.info("Fetching parks from BigQuery...")
        parks = []
        for row in client.query(query):
            parks.append({
                'park_code': row.park_code,
                'park_name': row.park_full_name,
                'park_url': row.park_url
            })
        
        logger.info(f"Found {len(parks)} parks in BigQuery")
        
        # Scrape trails
        return self.scrape_parks(parks)
    
    def scrape_parks(self, parks: List[Dict]) -> List[Trail]:
        """Scrape all parks for trail data"""
        all_trails = []
        
        for idx, park in enumerate(parks, 1):
            logger.info(f"Processing park {idx}/{len(parks)}: {park['park_name']}")
            
            try:
                park_trails = self.scrape_park(
                    park['park_url'],
                    park['park_code'],
                    park['park_name']
                )
                all_trails.extend(park_trails)
                logger.info(f"Found {len(park_trails)} trails for {park['park_name']}")
            except Exception as e:
                logger.error(f"Error scraping {park['park_name']}: {e}")
            
            time.sleep(self.delay)
        
        logger.info(f"Scraped {len(all_trails)} total trails")
        return all_trails
    
    def scrape_park(self, park_url: str, park_code: str, park_name: str) -> List[Trail]:
        """Scrape all trails for a single park"""
        trails = []
        visited_urls: Set[str] = set()
        scraped_at = datetime.utcnow().isoformat()
        
        # Get base URL
        base_url = self._get_base_url(park_url)
        
        # Try different common page patterns
        pages_to_check = [
            f"{base_url}/planyourvisit/hiking.htm",
            f"{base_url}/planyourvisit/day-hiking.htm",
            f"{base_url}/planyourvisit/trails.htm",
            f"{base_url}/planyourvisit/dayhikes.htm",
            f"{base_url}/planyourvisit/backcountry-hiking.htm",
            f"{base_url}/thingstodo.htm",
        ]
        
        # Also check the main planyourvisit page for links
        try:
            plan_page = f"{base_url}/planyourvisit/index.htm"
            soup = self._fetch_page(plan_page)
            if soup:
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    text = link.get_text().lower()
                    if any(keyword in text for keyword in ['hike', 'hikes', 'trail', 'trails', 'walk', 'walks']):
                        full_url = urljoin(plan_page, href)
                        if full_url not in pages_to_check:
                            pages_to_check.append(full_url)
        except Exception as e:
            logger.debug(f"Could not check planyourvisit index: {e}")
        
        # Check each page
        for url in pages_to_check:
            if url in visited_urls:
                continue
            visited_urls.add(url)
            
            try:
                page_trails = self._scrape_page(url, park_code, park_name)
                for trail in page_trails:
                    trail.scraped_at = scraped_at
                trails.extend(page_trails)
                time.sleep(self.delay)
            except Exception as e:
                logger.debug(f"Could not scrape {url}: {e}")
        
        return trails
    
    def _scrape_page(self, url: str, park_code: str, park_name: str) -> List[Trail]:
        """Scrape trails from a single page"""
        soup = self._fetch_page(url)
        if not soup:
            return []
        
        trails = []
        
        # Strategy 1: Look for structured trail cards/sections
        trail_containers = soup.find_all(['div', 'section', 'article'], 
                                        class_=re.compile(r'(trail|hike|activity|card)', re.I))
        
        for container in trail_containers:
            trail = self._parse_trail_container(container, park_code, park_name, url)
            if trail:
                trails.append(trail)
        
        # Strategy 2: Look for trail headings followed by descriptions
        headings = soup.find_all(['h2', 'h3', 'h4', 'h5'], string=re.compile(r'(trail|hike|loop|walk)', re.I))
        
        for heading in headings:
            trail = self._parse_trail_from_heading(heading, park_code, park_name, url)
            if trail:
                if not any(t.trail_name == trail.trail_name for t in trails):
                    trails.append(trail)
        
        # Strategy 3: Look for tables with trail information
        tables = soup.find_all('table')
        for table in tables:
            table_trails = self._parse_trail_table(table, park_code, park_name, url)
            for trail in table_trails:
                if not any(t.trail_name == trail.trail_name for t in trails):
                    trails.append(trail)
        
        return trails
    
    def _parse_trail_container(self, container, park_code: str, park_name: str, url: str) -> Optional[Trail]:
        """Parse a trail from a container element"""
        try:
            name_elem = container.find(['h2', 'h3', 'h4', 'h5'])
            if not name_elem:
                return None
            
            trail_name = name_elem.get_text().strip()
            
            description = ""
            desc_elem = container.find('p')
            if desc_elem:
                description = desc_elem.get_text().strip()
            
            text = container.get_text()
            
            return Trail(
                park_code=park_code,
                park_name=park_name,
                trail_name=trail_name,
                description=description,
                distance=self._extract_distance(text),
                duration=self._extract_duration(text),
                difficulty=self._extract_difficulty(text),
                elevation_gain=self._extract_elevation(text),
                trail_type=self._extract_trail_type(text),
                source_url=url
            )
        except Exception as e:
            logger.debug(f"Error parsing container: {e}")
            return None
    
    def _parse_trail_from_heading(self, heading, park_code: str, park_name: str, url: str) -> Optional[Trail]:
        """Parse trail information starting from a heading"""
        try:
            trail_name = heading.get_text().strip()
            trail_name = ' '.join(trail_name.split())
            
            description_parts = []
            current = heading.find_next_sibling()
            count = 0
            full_text = ""
            
            while current and count < 8:
                if current.name in ['h2', 'h3', 'h4', 'h5']:
                    break
                
                text = current.get_text().strip()
                if text:
                    full_text += " " + text
                    if len(text) > 15:
                        description_parts.append(text)
                
                current = current.find_next_sibling()
                count += 1
            
            description = ' '.join(description_parts[:3]) if description_parts else full_text.strip()
            
            if not description or len(description) < 20:
                return None
            
            return Trail(
                park_code=park_code,
                park_name=park_name,
                trail_name=trail_name,
                description=description,
                distance=self._extract_distance(full_text),
                duration=self._extract_duration(full_text),
                difficulty=self._extract_difficulty(full_text),
                elevation_gain=self._extract_elevation(full_text),
                trail_type=self._extract_trail_type(full_text),
                source_url=url
            )
        except Exception as e:
            logger.debug(f"Error parsing heading: {e}")
            return None
    
    def _parse_trail_table(self, table, park_code: str, park_name: str, url: str) -> List[Trail]:
        """Parse trails from a table structure"""
        trails = []
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return trails
            
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue
                
                row_text = ' '.join([cell.get_text().strip() for cell in cells])
                trail_name = cells[0].get_text().strip()
                
                trail = Trail(
                    park_code=park_code,
                    park_name=park_name,
                    trail_name=trail_name,
                    description=cells[1].get_text().strip() if len(cells) > 1 else "",
                    distance=self._extract_distance(row_text),
                    duration=self._extract_duration(row_text),
                    difficulty=self._extract_difficulty(row_text),
                    elevation_gain=self._extract_elevation(row_text),
                    trail_type=self._extract_trail_type(row_text),
                    source_url=url
                )
                
                if trail.trail_name:
                    trails.append(trail)
        
        except Exception as e:
            logger.debug(f"Error parsing table: {e}")
        
        return trails
    
    def _extract_distance(self, text: str) -> Optional[str]:
        """Extract distance from text"""
        patterns = [
            r'(\d+\.?\d*)\s*(?:miles?|mi\.?)',
            r'(\d+\.?\d*)\s*(?:kilometers?|km)',
            r'(\d+/\d+)\s*(?:miles?|mi\.?)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)
        return None
    
    def _extract_duration(self, text: str) -> Optional[str]:
        """Extract duration from text"""
        patterns = [
            r'\d+\.?\d*\s*-?\s*\d*\.?\d*\s*(?:hours?|hrs?)',
            r'\d+\s*(?:minutes?|mins?)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)
        return None
    
    def _extract_difficulty(self, text: str) -> Optional[str]:
        """Extract difficulty level from text"""
        difficulties = ['easy', 'moderate', 'strenuous', 'difficult', 'hard', 'challenging']
        text_lower = text.lower()
        for diff in difficulties:
            if diff in text_lower:
                return diff.capitalize()
        return None
    
    def _extract_elevation(self, text: str) -> Optional[str]:
        """Extract elevation gain from text"""
        patterns = [
            r'\d+,?\d*\s*(?:feet|ft\.?)\s*(?:elevation|gain)',
            r'(?:elevation|gain):\s*\d+,?\d*\s*(?:feet|ft\.?)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)
        return None
    
    def _extract_trail_type(self, text: str) -> Optional[str]:
        """Extract trail type"""
        types = ['loop', 'out-and-back', 'out and back', 'point-to-point', 'lollipop']
        text_lower = text.lower()
        for trail_type in types:
            if trail_type in text_lower:
                return trail_type.title()
        return None
    
    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a page"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            logger.debug(f"Could not fetch {url}: {e}")
            return None
    
    def _get_base_url(self, url: str) -> str:
        """Extract base URL from park URL"""
        return url.rsplit('/', 1)[0] if url.endswith('.htm') else url.rstrip('/')


def load_to_bigquery(trails: List[Trail], project_id: str, dataset_id: str, table_id: str = "nps_trails"):
    """Load trail data to BigQuery"""
    logger.info(f"Loading {len(trails)} trails to BigQuery...")
    
    # Set up credentials
    credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if credentials_json:
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project=project_id)
    else:
        client = bigquery.Client(project=project_id)
    
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Define schema
    schema = [
        bigquery.SchemaField("park_code", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("park_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trail_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("distance", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("duration", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("difficulty", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("elevation_gain", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("trail_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="NULLABLE"),
    ]
    
    # Create or get table
    try:
        table = client.get_table(table_ref)
        logger.info(f"Table {table_id} already exists")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")
    
    # Convert trails to BigQuery rows
    rows = [trail.to_bigquery_row() for trail in trails]
    
    # Configure job to replace existing data
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    
    # Load data
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    
    logger.info(f"✓ Loaded {len(trails)} trails to {table_id}")


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape NPS trail information')
    parser.add_argument('--project-id', default=os.getenv('PROJECT_ID'), help='GCP Project ID')
    parser.add_argument('--dataset-id', default=os.getenv('DATASET_ID'), help='BigQuery Dataset ID')
    parser.add_argument('--delay', type=float, default=1.5, help='Delay between requests')
    parser.add_argument('--output', default='trails.json', help='Output JSON file (optional)')
    
    args = parser.parse_args()
    
    if not args.project_id or not args.dataset_id:
        logger.error("PROJECT_ID and DATASET_ID must be provided via args or env vars")
        return
    
    # Scrape trails
    scraper = NPSTrailScraper(delay=args.delay)
    trails = scraper.scrape_parks_from_bigquery(args.project_id, args.dataset_id)
    
    # Save to JSON (optional)
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump([trail.to_dict() for trail in trails], f, indent=2, ensure_ascii=False)
        logger.info(f"Saved trails to {args.output}")
    
    # Load to BigQuery
    if trails:
        load_to_bigquery(trails, args.project_id, args.dataset_id)
    else:
        logger.warning("No trails found to load")


if __name__ == '__main__':
    main()