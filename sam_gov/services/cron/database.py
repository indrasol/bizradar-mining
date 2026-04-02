import psycopg2
from sam_gov.utils.db_utils import get_db_connection
from sam_gov.utils.logger import get_logger

# Configure logging
logger = get_logger(__name__)

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

def insert_data(rows):
    """
    Inserts multiple rows into the sam_gov table, avoiding duplicates.
    
    Args:
        rows: List of dictionaries containing data to insert
    
    Returns:
        dict: Summary with counts of inserted and skipped records
    """
    connection = get_db_connection()
    if not connection:
        return {"error": "Could not connect to database", "inserted": 0, "skipped": 0}
    
    inserted = 0
    skipped = 0
    
    try:
        with connection.cursor() as cursor:
            insert_query = """
                INSERT INTO sam_gov
                (title, department, published_date, response_date, naics_code, description,
                 notice_id, solicitation_number, url, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            for row in rows:
                notice_id = row.get("notice_id")
                
                # Skip if notice_id is missing (shouldn't happen but just in case)
                if not notice_id:
                    logger.warning("Skipping row with missing notice_id")
                    skipped += 1
                    continue
                
                # Check if this record already exists
                if check_duplicate(cursor, notice_id):
                    logger.info(f"Skipping duplicate record with notice_id: {notice_id}")
                    skipped += 1
                    continue
                
                # Insert the record if it doesn't exist
                try:
                    cursor.execute(insert_query, (
                        row["title"],
                        row["department"],
                        row["published_date"],
                        row["response_date"],
                        row["naics_code"],
                        row["description"],
                        notice_id,
                        row["solicitation_number"],
                        row["url"],
                        row["active"]
                    ))
                    inserted += 1
                except psycopg2.Error as e:
                    logger.error(f"Error inserting record {notice_id}: {e}")
                    skipped += 1
                    # Continue with other records even if one fails
                    continue
                
        connection.commit()
        logger.info(f"Database insertion complete. Inserted: {inserted}, Skipped duplicates: {skipped}")
        return {"inserted": inserted, "skipped": skipped}
    
    except psycopg2.Error as e:
        connection.rollback()
        logger.error(f"Error during database transaction: {e}")
        return {"error": str(e), "inserted": inserted, "skipped": skipped}
    finally:
        connection.close()