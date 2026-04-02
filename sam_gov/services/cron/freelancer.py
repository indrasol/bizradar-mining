import logging
import json
import argparse
from typing import List, Dict, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
from sam_gov.utils.db_utils import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_session() -> requests.Session:
    """Configure and return a requests session with retry strategy."""
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session

def scrape_projects(session: requests.Session, base_url: str, max_pages: int = 10) -> List:
    """Scrape project listings from multiple pages."""
    projects = []
    for page in range(1, max_pages + 1):
        url = f"{base_url}{page}"
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            page_projects = soup.find_all("div", class_="JobSearchCard-item")
            
            if not page_projects:
                logger.warning(f"No projects found on page {page}")
                break
                
            projects.extend(page_projects)
            logger.info(f"Scraped page {page}: {len(page_projects)} projects")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to scrape page {page}: {e}")
            continue
    
    logger.info(f"Total projects scraped from {page} pages: {len(projects)}")
    return projects

def extract_project_data(project) -> Optional[Dict]:
    """Extract relevant data from a single project listing."""
    try:
        title_elem = project.find("a", class_="JobSearchCard-primary-heading-link")
        title = title_elem.text.strip() if title_elem else "No Title"
        job_url = f"https://www.freelancer.com{title_elem['href']}" if title_elem and title_elem.has_attr("href") else None

        published_elem = project.find("span", class_="JobSearchCard-primary-heading-days")
        published_date = published_elem.text.strip() if published_elem else "No Published Date"

        price_elem = project.find("div", class_="JobSearchCard-secondary-price")
        price = re.sub(r"\s+Avg Bid\s*", "", price_elem.text.strip()) if price_elem else "No Price"

        bids_elem = project.find("div", class_="JobSearchCard-secondary-entry")
        bids = re.sub(r"\s+Bids\s*", "", bids_elem.text.strip()) if bids_elem else "No Bids"

        skills = ", ".join(
            skill.text.strip() for skill in project.find_all("a", class_="JobSearchCard-primary-tagsLink")
        ) or "No Skills Listed"

        details_elem = project.find("p", class_="JobSearchCard-primary-description")
        details = re.sub(r"\s+", " ", details_elem.text.strip()) if details_elem else "No Additional Details"

        return {
            "Job URL": job_url,
            "Title": title,
            "Published Date": published_date,
            "Skills Required": skills,
            "Price/Budget": price,
            "Bids so Far": bids,
            "Additional Details": details
        }
    except Exception as e:
        logger.error(f"Error extracting project data: {e}")
        return None

def clean_price(price: str) -> float:
    """Clean and convert price string to float."""
    if pd.isnull(price) or price == "No Price":
        return np.nan
    cleaned = re.sub(r'[^\d.]', '', str(price))
    try:
        return float(cleaned)
    except ValueError:
        return np.nan

def convert_to_hours(text: str) -> float:
    """Convert published date text to hours."""
    text = text.lower()
    match = re.search(r'(\d+)', text)
    if not match:
        return np.nan
    num = int(match.group(1))
    return num if 'hour' in text else num * 24 if 'day' in text else np.nan

def process_data(projects: List) -> pd.DataFrame:
    """Process and clean scraped project data."""
    projects_data = [data for project in projects if (data := extract_project_data(project))]
    df = pd.DataFrame(projects_data)
    
    if df.empty:
        logger.warning("No project data to process")
        return df

    # Clean and preprocess
    df = df.apply(lambda x: x.str.replace('\x00', '') if x.dtype == "object" else x)
    
    # Normalize text columns
    text_cols = ['Title', 'Published Date', 'Skills Required', 'Bids so Far', 'Additional Details']
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()
    df['Title'] = df['Title'].str.title()

    # Clean numeric columns
    df['Price/Budget'] = df['Price/Budget'].apply(clean_price)
    df['Bids so Far'] = df['Bids so Far'].str.extract(r'(\d+)').astype(float)
    df['Hours Left'] = df['Published Date'].apply(convert_to_hours)
    
    # Remove duplicates
    df.drop_duplicates(subset=['Job URL'], keep='first', inplace=True)
    
    return df

def save_to_database(df: pd.DataFrame, record_id: str, trigger_type: str) -> tuple[int, int]:
    """Save cleaned data to Supabase PostgreSQL database and return counts."""
    total_count = len(df)
    new_count = 0
    
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            logger.info("Connected to PostgreSQL successfully")
            
            # Create table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS freelancer_data_table (
                    id SERIAL PRIMARY KEY,
                    job_url TEXT UNIQUE,
                    title TEXT,
                    published_date TEXT,
                    skills_required TEXT,
                    price_budget TEXT,
                    bids_so_far TEXT,
                    additional_details TEXT,
                    record_id TEXT,
                    trigger_type TEXT
                );
            ''')
            conn.commit()
            logger.info("Table ensured: freelancer_data_table")

            # Insert data and count new records
            for _, row in df.iterrows():
                insert_query = '''
                    INSERT INTO freelancer_data_table (
                        job_url, title, published_date, skills_required,
                        price_budget, bids_so_far, additional_details,
                        record_id, trigger_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_url) DO NOTHING
                    RETURNING id;
                '''
                cursor.execute(insert_query, (
                    row['Job URL'],
                    row['Title'],
                    row['Published Date'],
                    row['Skills Required'],
                    str(row['Price/Budget']),
                    str(row['Bids so Far']),
                    row['Additional Details'],
                    record_id,
                    trigger_type
                ))
                if cursor.fetchone():  # If a row was inserted
                    new_count += 1
            
            conn.commit()
            logger.info(f"Inserted {new_count} new records out of {total_count} total records")

            # Preview data
            cursor.execute("SELECT * FROM freelancer_data_table LIMIT 5;")
            rows = cursor.fetchall()
            for row in rows:
                logger.info(f"Preview row: {row}")
                
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        raise
    
    return total_count, new_count

def main(record_id: str, trigger_type: str) -> dict:
    """Main function to orchestrate the scraping process."""
    try:
        session = setup_session()
        base_url = "https://www.freelancer.com/search/projects?projectLanguages=en&projectSkills=305&page="
        projects = scrape_projects(session, base_url)
        if not projects:
            logger.warning("No projects scraped")
            return {"count": 0, "new_count": 0, "status": "success"}
            
        df = process_data(projects)
        if df.empty:
            logger.warning("No data processed")
            return {"count": 0, "new_count": 0, "status": "success"}
            
        total_count, new_count = save_to_database(df, record_id, trigger_type)
        
        return {
            "count": total_count,
            "new_count": new_count,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Scraping process failed: {e}")
        return {
            "count": 0,
            "new_count": 0,
            "status": f"failed: {str(e)}"
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Freelancer.com scraper")
    parser.add_argument("--record-id", required=True, help="ETL record ID")
    parser.add_argument("--trigger-type", required=True, help="Trigger type")
    args = parser.parse_args()
    
    result = main(args.record_id, args.trigger_type)
    print(json.dumps(result))