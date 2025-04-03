import psycopg2
from datetime import datetime
from multithread_file_loader import FileLoader
import configparser
import concurrent.futures
from src import logger
from sqlalchemy import create_engine
import sys
import os
from database_connection import connect_to_db

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))



def insert_pipeline_log(cur, log_id, start_time):
    insert_pipeline_query = """
        INSERT INTO file_ingestion_pipeline_log (log_id, start_time, status, process_type) 
        VALUES (%s, %s, %s, %s)
    """
    cur.execute(insert_pipeline_query, (log_id, start_time, 'Job Started', 'file ingestion'))

def update_pipeline_log(cur, log_id:str, end_time, status, error_message=None, records_processed=None):
    update_pipeline_query = """
        UPDATE file_ingestion_pipeline_log 
        SET end_time = %s, status = %s, error_message = %s, records_processed = %s
        WHERE log_id = %s
    """
    cur.execute(update_pipeline_query, (end_time, status, error_message, records_processed, log_id))

def insert_facility_uploads():
    """
    Inserts unprocessed facility uploads into the batch_facility_processing table.
    """
    PROCESSED = 1
    MODIFIED_DATE = '2025-01-01'
    insert_query = """
        INSERT INTO batch_facility_processing(facility_id, status, file_count)
        SELECT facility_id, 'UNPROCESSED' AS status, COUNT(1) AS file_count
        FROM (
            SELECT facility_id, decrypted_file_name
            FROM sync_file
            WHERE processed = 1
              AND modified_date >= '2025-01-01'
            LIMIT 50
        ) z
        WHERE NOT (
            decrypted_file_name ILIKE ANY (
                ARRAY[
                    'prep_eligibility_%', 'prep_clinic_%',
                    'mhpss_confirmation_%', 'pmtct_anc_%',
                    'dsd_devolvement_%', 'hiv_art_clinical_%'
                ]
            )
        )
        GROUP BY facility_id
    """

    try:
        with connect_to_db.connect('filedb')[0] as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query)
                conn.commit()
                logger.info('Facility batch file uploads inserted into batch_facility_processing')
    except psycopg2.Error as e:
        logger.exception(f"Error inserting facility uploads into batch_facility_processing {e}", exc_info=True)


def update_facility_uploads(cur_status,start_time,end_time,error_message,facility_id,batch_id, prev_status):
    update_query = """
                   UPDATE batch_facility_processing
                   SET status=%s, start_time=%s, end_time=%s, error_message=%s
                   WHERE facility_id=%s AND id=%s AND status=%s
                   """
    try:
        with connect_to_db.connect('filedb')[0] as conn:
            with conn.cursor() as cur:
                cur.execute(update_query, (cur_status, start_time, end_time, error_message, 
                                            facility_id, batch_id, prev_status))
                conn.commit()
                logger.info(f'files updated for {facility_id} in batch_facility_processing')
    
    except psycopg2.Error as e:
        logger.exception(f"Error connecting to PostgreSQL database and updating records {e}", exc_info=True)
    
def create_single_instance(facility:tuple):
    column_names = ["batch_id","facility_id","file_count"]
    facility_info = dict(zip(column_names, facility))
    batch_id = facility_info['batch_id']
    facility_id = facility_info['facility_id']
    file_count = facility_info['file_count']
    
    try:
        start_time = datetime.now()
        logger.info(f'ingestion of {file_count} files for {facility_id} started')
        loader = FileLoader()
        loader._retrieve_localdir_from_syncfile(facility_id)
        end_time = datetime.now()
        update_facility_uploads('PROCESSED', start_time, end_time, 'No errors', facility_id, batch_id, 'UNPROCESSED')
        logger.info(f'ingestion of {file_count} files for {facility_id} completed')
    
    except Exception as e:
        end_time = datetime.now()  # Ensure end_time is always set
        update_facility_uploads('FAILED', start_time, end_time, str(e), facility_id, batch_id, 'UNPROCESSED')
        logger.exception(f'ingestion of {file_count} files for {facility_id} failed', exc_info=True)

def process_facilities_in_batches(facilities, batch_size=20):
    """
    Processes facilities in batches using multithreading.
    """
    try:
        with connect_to_db.connect('lamisplus_staging_dwh')[0] as conn:
            with conn.cursor() as cur:
                start_time = datetime.now()
                log_id = f'IPID_{start_time.strftime("%Y%m%d_%H_%M")}'
                logger.info(f"Starting ingestion process with log ID: {log_id}")

                insert_pipeline_log(cur, log_id, start_time)

                for i in range(0, len(facilities), batch_size):
                    batch = facilities[i:i + batch_size]
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        executor.map(create_single_instance, batch)

                end_time = datetime.now()
                update_pipeline_log(cur, log_id, end_time, 'Job Passed', 'No Errors', len(facilities))
                logger.info("File ingestion process completed successfully.")

    except Exception as e:
        logger.exception("Error processing facilities in batches {e}", exc_info=True)

def main():
    """
    Main function to orchestrate the file ingestion process.
    """
    try:
        insert_facility_uploads()

        with connect_to_db.connect('filedb')[0] as filedb_conn:
            with filedb_conn.cursor() as filedb_cur:
                filedb_cur.execute("""
                    SELECT id, facility_id, file_count 
                    FROM batch_facility_processing 
                    WHERE status = 'UNPROCESSED'
                """)
                facilities = filedb_cur.fetchall()

        if facilities:
            process_facilities_in_batches(facilities)
        else:
            logger.info("No unprocessed facilities found.")

    except Exception as e:
        logger.exception(f"An error occurred in the main function {e}", exc_info=True)


if __name__ == '__main__':
    main()
