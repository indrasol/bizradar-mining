"""
ETL History update module for tracking workflow status and results
"""
import argparse
from sam_gov.utils.db_utils import get_db_connection
from sam_gov.utils.logger import get_logger

# Configure logging
logger = get_logger('etl_history_updater',True)

def update_etl_history(
    record_id=None,
    status=None,
    sam_gov_count=0,
    sam_gov_new=0,
    freelancer_count=0,
    freelancer_new=0,
    trigger_type=None
):
    """
    Update ETL history record with collection results
    
    Args:
        record_id (int): ETL history record ID to update
        status (str): Status of the ETL run (success, failed, partial)
        sam_gov_count (int): Total SAM.gov records processed
        sam_gov_new (int): New SAM.gov records added
        freelancer_count (int): Total Freelancer records processed
        freelancer_new (int): New Freelancer records added
        trigger_type (str): Type of trigger that initiated the workflow
    
    Returns:
        bool: True if update was successful, False otherwise
    """
    if not record_id:
        logger.error("No record ID provided. Cannot update ETL history.")
        return False
    
    try:
        # Connect to the database        
        conn = get_db_connection()
        cursor = conn.cursor()
        logger.info(f"Connected to database. Updating ETL history record {record_id}")
        
        # Calculate total records
        total_records = int(sam_gov_count) + int(freelancer_count)
        
        # Prepare update query based on whether trigger_type is provided
        if trigger_type:
            update_query = """
            UPDATE etl_history 
            SET 
                status = %s,
                sam_gov_count = %s,
                sam_gov_new_count = %s,
                freelancer_count = %s,
                freelancer_new_count = %s,
                total_records = %s,
                trigger_type = %s
            WHERE id = %s
            """
            cursor.execute(update_query, (
                status,
                sam_gov_count,
                sam_gov_new,
                freelancer_count,
                freelancer_new,
                total_records,
                trigger_type,
                record_id
            ))
        else:
            # Get existing trigger_type if not provided
            cursor.execute("SELECT trigger_type FROM etl_history WHERE id = %s", (record_id,))
            result = cursor.fetchone()
            existing_trigger_type = result[0] if result else 'ui-manual'
            
            update_query = """
            UPDATE etl_history 
            SET 
                status = %s,
                sam_gov_count = %s,
                sam_gov_new_count = %s,
                freelancer_count = %s,
                freelancer_new_count = %s,
                total_records = %s
            WHERE id = %s
            """
            cursor.execute(update_query, (
                status,
                sam_gov_count,
                sam_gov_new,
                freelancer_count,
                freelancer_new,
                total_records,
                record_id
            ))
        
        conn.commit()
        
        # Log the update
        logger.info(f"ETL history record {record_id} updated successfully")
        logger.info(f"Status: {status}")
        logger.info(f"SAM.gov: {sam_gov_count} records ({sam_gov_new} new)")
        logger.info(f"Freelancer: {freelancer_count} records ({freelancer_new} new)")
        logger.info(f"Total records: {total_records}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error updating ETL history record: {str(e)}")
        return False

def main():
    """
    Main function to parse arguments and update ETL history
    """
    parser = argparse.ArgumentParser(description='Update ETL history record with results')
    parser.add_argument('--record-id', type=int, required=True, help='ETL history record ID to update')
    parser.add_argument('--status', choices=['success', 'failed', 'partial'], default='success', 
                        help='Status of the ETL run')
    parser.add_argument('--sam-gov-count', type=int, default=0, help='Total SAM.gov records processed')
    parser.add_argument('--sam-gov-new', type=int, default=0, help='New SAM.gov records added')
    parser.add_argument('--freelancer-count', type=int, default=0, help='Total Freelancer records processed')
    parser.add_argument('--freelancer-new', type=int, default=0, help='New Freelancer records added')
    parser.add_argument('--trigger-type', choices=['ui-manual', 'github-manual', 'github-scheduled'],
                       help='Type of trigger that initiated the workflow')
    
    args = parser.parse_args()
    
    # Update ETL history record
    success = update_etl_history(
        record_id=args.record_id,
        status=args.status,
        sam_gov_count=args.sam_gov_count,
        sam_gov_new=args.sam_gov_new,
        freelancer_count=args.freelancer_count,
        freelancer_new=args.freelancer_new,
        trigger_type=args.trigger_type
    )
    
    import sys
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()