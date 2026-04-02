import urllib.parse
from typing import Dict, Any
import aiohttp
import ssl
import certifi
from datetime import datetime, timedelta
import asyncio
import psycopg2

from sam_gov.utils.logger import get_logger
from sam_gov.utils.db_utils import get_db_connection

# Configure logging
logger = get_logger(__name__)

# === Database Functions (from database.py) ===

def check_duplicate(cursor, notice_id):
    """
    Check if a record with the given notice_id already exists in the database.
    
    Args:
        cursor: Database cursor
        notice_id: The notice_id to check
        
    Returns:
        bool: True if the record exists, False otherwise
    """
    query = "SELECT 1 FROM sam_gov WHERE notice_id = %s LIMIT 1"
    cursor.execute(query, (notice_id,))
    return cursor.fetchone() is not None

def deduplicate_solicitation_number(cursor, solicitation_number, archived_by="dedup_script"):
    """
    Detects duplicates for a given solicitation_number in sam_gov.
    If more than one exists, moves all except the latest (by id) to sam_gov_history.
    
    Args:
        cursor: psycopg2 cursor
        solicitation_number: The solicitation_number to check
        archived_by: Optional, sets who archived the record
    """
    
    # Step 1: Get all matching IDs ordered by id descending (latest first)
    fetch_query = """
        SELECT id FROM sam_gov
        WHERE solicitation_number = %s
        ORDER BY id DESC
    """
    cursor.execute(fetch_query, (solicitation_number,))
    rows = cursor.fetchall()
    
    # If 0 or 1 records, nothing to do
    if len(rows) <= 1:
        return False  # No duplicates found

    # Keep the first (latest), archive the rest
    ids_to_archive = [row[0] for row in rows[1:]]  # Skip latest

    # Step 2: Move to history
    insert_query = f"""
        INSERT INTO sam_gov_history (
            id, notice_id, solicitation_number, title, department,
            naics_code, published_date, response_date, description,
            url, active, created_at, updated_at, additional_description,
            archived_at, archived_by
        )
        SELECT
            id, notice_id, solicitation_number, title, department,
            naics_code, published_date, response_date, description,
            url, active, created_at, updated_at, additional_description,
            CURRENT_TIMESTAMP, %s
        FROM sam_gov
        WHERE id = ANY(%s)
    """
    cursor.execute(insert_query, (archived_by, ids_to_archive))

    # Step 3: Delete from sam_gov
    delete_query = "DELETE FROM sam_gov WHERE id = ANY(%s)"
    cursor.execute(delete_query, (ids_to_archive,))

    return True  # Duplicates handled

def upsert_with_history(cursor, row, archived_by="upsert_script"):
    """
    Upsert a record into sam_gov. If a record with the same notice_id exists and any field changes, move the old record to sam_gov_history before inserting the new one.
    """
    # 1. Check if record exists
    cursor.execute("SELECT * FROM sam_gov WHERE notice_id = %s", (row["notice_id"],))
    existing = cursor.fetchone()

    # By default, we don't insert. We only insert if the record is new,
    # or if it has changed and the old version has been deleted.
    should_insert = False

    if not existing:
        # Case 1: Record is new. We should insert it.
        should_insert = True
    else:
        # Case 2: Record exists. Check for changes.
        colnames = [desc[0] for desc in cursor.description]
        changed = False
        for idx, colname in enumerate(colnames):
            api_val = row.get(colname)
            db_val = existing[idx]
            
            # psycopg2 returns date objects for date columns.
            # Our `parse_date` also creates date objects, so types should match.
            if colname in row and api_val != db_val:
                logger.info(f"Change detected for notice_id {row['notice_id']} in field '{colname}'. DB: '{db_val}', API: '{api_val}'")
                changed = True
                break
        
        if changed:
            # If changed, move old record to history...
            insert_history_query = """
                INSERT INTO sam_gov_history (
                    id, notice_id, solicitation_number, title, department,
                    naics_code, published_date, response_date, description,
                    url, active, created_at, updated_at, additional_description,
                    archived_at, archived_by
                )
                SELECT
                    id, notice_id, solicitation_number, title, department,
                    naics_code, published_date, response_date, description,
                    url, active, created_at, updated_at, additional_description,
                    CURRENT_TIMESTAMP, %s
                FROM sam_gov WHERE notice_id = %s
            """
            cursor.execute(insert_history_query, (archived_by, row["notice_id"]))
            
            # ...delete the old record...
            cursor.execute("DELETE FROM sam_gov WHERE notice_id = %s", (row["notice_id"],))
            
            # ...and flag that the new version should be inserted.
            should_insert = True
    
    # 3. Insert new/updated record ONLY if it's new or has changed.
    if should_insert:
        insert_query = """
            INSERT INTO sam_gov
            (title, department, published_date, response_date, naics_code, description,
             notice_id, solicitation_number, url, active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            row["title"],
            row["department"],
            row["published_date"],
            row["response_date"],
            row["naics_code"],
            row["description"],
            row["notice_id"],
            row["solicitation_number"],
            row["url"],
            row["active"]
        ))


def insert_data(rows):
    """
    Inserts or updates multiple rows into the sam_gov table, moving old records to history if any field changes.
    """
    connection = get_db_connection()
    if not connection:
        return {"error": "Could not connect to database", "inserted": 0, "skipped": 0}
    
    inserted = 0
    skipped = 0
    
    try:
        with connection.cursor() as cursor:
            for row in rows:
                notice_id = row.get("notice_id")
                # Skip if notice_id is missing
                if not notice_id:
                    logger.warning("Skipping row with missing notice_id")
                    skipped += 1
                    continue
                try:
                    upsert_with_history(cursor, row)
                    inserted += 1
                    # Deduplicate any old solicitation_number entries before inserting new one
                    solicitation_number = row.get("solicitation_number")
                    if solicitation_number:
                        deduplicate_solicitation_number(cursor, solicitation_number)
                except Exception as e:
                    logger.error(f"Error upserting record {notice_id}: {e}")
                    skipped += 1
                    continue
        connection.commit()
        logger.info(f"Database upsert complete. Inserted/Updated: {inserted}, Skipped: {skipped}")
        return {"inserted": inserted, "skipped": skipped}
    except Exception as e:
        connection.rollback()
        logger.error(f"Error during database transaction: {e}")
        return {"error": str(e), "inserted": inserted, "skipped": skipped}
    finally:
        connection.close()

# === SAM.gov Functions ===

def parse_date(date_str: str):
    """Parse a date string (ISO 8601 format with timezone) into a date object."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
    except ValueError as e:
        logger.error(f"Date parsing error for {date_str}: {e}")
        return None

def truncate_string(text, max_length=255):
    """Truncate a string to specified maximum length."""
    if not text:
        return text
    return text[:max_length]

async def fetch_opportunities() -> Dict[str, Any]:
    """
    Fetch opportunities from SAM.gov API for multiple NAICS codes and save to database.
    
    Returns:
        Dictionary with results summary
    """
    # Get SAM.gov API key from environment variable
    # api_key = os.getenv("SAM_API_KEY")
    from sam_gov.config.settings import SAM_API_KEY as api_key
    
    if not api_key:
        logger.error("SAM.gov API key not found in environment variables.")
        return {"source": "sam.gov", "count": 0, "error": "API key missing"}

    base_url = "https://api.sam.gov/prod/opportunities/v2/search"
    
    # Fetch a full year window
    posted_to = datetime.now().strftime('%m/%d/%Y')
    posted_from = (datetime.now() - timedelta(days=180)).strftime('%m/%d/%Y')
    
    logger.info(f"Searching for opportunities from {posted_from} to {posted_to}")

    # Top NAICS codes to search (from our Colab implementation)
    naics_list = ["541512", "541611", "541519","541715","518210"]
    
    all_opportunities = []  # Store all collected opportunities here
    total_fetched = 0
    records_per_naics = 1000  # Number of records per API call (max allowed)

    for i, naics in enumerate(naics_list, 1):
        offset = 0
        fetched_for_naics = 0
        while True:
            params = {
                "api_key": api_key,
                "ncode": naics,
                "postedFrom": posted_from,
                "postedTo": posted_to,
                "limit": records_per_naics,  # Only request what we need
                "offset": offset,
                "ptype": "o,k,p,r,s" # Only contract opportunity types
            }
            
            log_params = params.copy()
            log_params["api_key"] = "***REDACTED***"
            log_url = f"{base_url}?{urllib.parse.urlencode(log_params, safe='/')}"
            logger.info(f"Call {i}/{len(naics_list)} (offset {offset}): Fetching for NAICS {naics} from {log_url}")

            ssl_context = ssl.create_default_context(cafile=certifi.where())
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(base_url, params=params, ssl=ssl_context, timeout=60) as response:
                        response_text = await response.text()
                        if response.status == 200:
                            try:
                                data = await response.json()
                                current_opps = data.get("opportunitiesData", [])
                                
                                if not current_opps:
                                    logger.info(f"No more contract opportunities found for NAICS {naics} at offset {offset}")
                                    break
                                
                                # Add NAICS code to each opportunity for reference
                                for opp in current_opps:
                                    opp['naics_code'] = naics
                                
                                all_opportunities.extend(current_opps)
                                fetched_for_naics += len(current_opps)
                                total_fetched += len(current_opps)
                                logger.info(f"Fetched {len(current_opps)} opportunities for NAICS {naics} (offset {offset}), Total for NAICS: {fetched_for_naics}, Grand Total: {total_fetched}")
                                
                                # If less than limit, this is the last page
                                if len(current_opps) < records_per_naics:
                                    break
                                else:
                                    offset += records_per_naics
                                
                            except Exception as json_error:
                                logger.error(f"Error parsing JSON response: {json_error}")
                                logger.error(f"Response text: {response_text[:500]}")
                                break
                        else:
                            logger.error(f"Error fetching for NAICS {naics} (offset {offset}): HTTP {response.status} - {response_text}")
                            break
                # Add a delay between requests to respect rate limits
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Exception for NAICS {naics} (offset {offset}): {str(e)}")
                break

    # Format data for database insertion
    if all_opportunities:
        rows = []
        for opp in all_opportunities:
            # Get NAICS code (integer)
            naics = opp.get("naics_code")
            if naics:
                try:
                    naics = int(naics)
                except ValueError:
                    naics = None
            
            notice_id = str(opp.get("noticeId", "")).strip()
            
            # Format data to match table schema
            row = {
                "notice_id": notice_id,
                "solicitation_number": opp.get("solicitationNumber"),
                "title": truncate_string(opp.get("title", "No title")),
                "department": truncate_string(opp.get("fullParentPathName", opp.get("department", ""))),
                "naics_code": naics,
                "published_date": parse_date(opp.get("postedDate")),
                "response_date": parse_date(opp.get("responseDeadLine")),
                "description": opp.get("description", ""),
                "url": f"https://sam.gov/opp/{notice_id}/view" if notice_id else None,
                # Normalize 'active' to boolean: True if 'Yes', False otherwise
                "active": True if str(opp.get("active", "Yes")).strip().lower() == "yes" else False
            }
            rows.append(row)
        
        if rows:
            logger.info(f"Preparing to insert {len(rows)} opportunities into database")
            result = insert_data(rows)
            
            # Return detailed results
            db_results = {
                "source": "sam.gov", 
                "total_fetched": total_fetched,
                "processed": len(rows),
                "inserted": result.get("inserted", 0),
                "skipped": result.get("skipped", 0),
                "error": result.get("error")
            }
            
            # Always run indexing for ALL records in the database
            try:
                # Import indexing function and run indexing for ALL data
                logger.info("Running Pinecone indexing for ALL records...")
                from sam_gov.utils.index_to_pinecone import index_sam_gov_to_pinecone, cleanup_to_only_sam_gov_vectors
                # Run indexing for ALL SAM.gov records
                index_result = index_sam_gov_to_pinecone(incremental=False)
                db_results["indexed_count"] = index_result
                logger.info(f"Successfully indexed {index_result} records to Pinecone")
                # Run full vector cleanup (remove non-sam_gov and orphaned sam_gov vectors)
                logger.info("Running Pinecone full cleanup (keep only valid sam_gov vectors)...")
                deleted_count = cleanup_to_only_sam_gov_vectors()
                logger.info(f"Deleted {deleted_count} Pinecone vectors (non-sam_gov and orphaned sam_gov).")
            except ImportError as e:
                logger.warning(f"Could not import utils.index_to_pinecone module: {e}")
                logger.warning("Pinecone indexing will be skipped for this run")
                db_results["indexed_count"] = 0
                
            except Exception as e:
                logger.error(f"Error during Pinecone indexing: {e}")
                db_results["indexing_error"] = str(e)

            # --- Post-ETL: Mark records as inactive if not in latest API fetch ---
            try:
                latest_notice_ids = set(row["notice_id"] for row in rows if row.get("notice_id"))
                connection = get_db_connection()
                if connection and latest_notice_ids:
                    with connection.cursor() as cursor:
                        # Use tuple for SQL IN clause
                        notice_id_tuple = tuple(latest_notice_ids)
                        # If only one element, add a trailing comma for SQL syntax
                        if len(notice_id_tuple) == 1:
                            notice_id_tuple = (notice_id_tuple[0], "dummy")
                        sql = f"""
                            UPDATE sam_gov
                            SET active = FALSE
                            WHERE active = TRUE
                              AND notice_id NOT IN %s
                        """
                        cursor.execute(sql, (notice_id_tuple,))
                        marked_inactive = cursor.rowcount
                    connection.commit()
                    logger.info(f"Marked {marked_inactive} records as inactive (not present in latest API fetch)")
                    db_results["marked_inactive"] = marked_inactive
                elif not latest_notice_ids:
                    logger.warning("No notice_ids found in latest API fetch for inactive marking step.")
                else:
                    logger.error("Could not connect to database for inactive marking step.")
            except Exception as e:
                logger.error(f"Error during post-ETL inactive marking step: {e}")
                db_results["inactive_marking_error"] = str(e)
                
            return db_results
    
    return {"source": "sam.gov", "count": 0, "status": "No opportunities found"}

# Function to handle command line arguments
def parse_args():
    """Parse command line arguments"""
    import argparse
    parser = argparse.ArgumentParser(description='SAM.gov data collection script')
    parser.add_argument('--record-id', type=int, help='ETL record ID')
    parser.add_argument('--trigger-type', type=str, help='Trigger type (scheduled or manual)')
    return parser.parse_args()

# For running as a script
if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    if args.record_id:
        logger.info(f"Running with ETL record ID: {args.record_id}, trigger type: {args.trigger_type}")
    
    # Run the async function
    result = asyncio.run(fetch_opportunities())
    
    # Calculate counts for output
    count = result.get("total_fetched", 0)
    new_count = result.get("inserted", 0) 
    status = "error" if result.get("error") else "success"
    
    # Output in JSON format for the GitHub workflow
    output = {
        "count": count,
        "new_count": new_count, 
        "status": status
    }
    
    print(output)