import psycopg2
from datetime import datetime
import logging
from dsd_loader import FileLoader
from src import logger
import configparser

def read_db_config(filename='/home/lamisplus/database_credentials/config.ini', section='database'):
    # Create a parser
    parser = configparser.ConfigParser()
    # Read the configuration file
    parser.read(filename)
    # Get section, default to database
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return db

db_config = read_db_config()


def insert_pipeline_log(cur, log_id, start_time):
    insert_pipeline_query = """
        INSERT INTO file_ingestion_pipeline_log (log_id, start_time, status, process_type) 
        VALUES (%s, %s, %s, %s)
    """
    cur.execute(insert_pipeline_query, (log_id, start_time, 'Job Started', 'file ingestion'))

def update_pipeline_log(cur, log_id, end_time, status, error_message=None, records_processed=None):
    update_pipeline_query = """
        UPDATE file_ingestion_pipeline_log 
        SET end_time = %s, status = %s, error_message = %s, records_processed = %s
        WHERE log_id = %s
    """
    cur.execute(update_pipeline_query, (end_time, status, error_message, records_processed, log_id))

def connect_to_db(db_params):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        return conn, cur
    except psycopg2.Error as e:
        logger.exception(f"Error connecting to PostgreSQL database: {e}")
        raise

def main():
    db_params = {
        'host': db_config['stg_host'],
        'database': db_config['stg_database_name'],
        'user': db_config['stg_username'],
        'password': db_config['stg_password'],
        'port': db_config['stg_port'],
    }

    try:
        conn, cur = connect_to_db(db_params)
        start_time = datetime.now() 
        log_id = f'IPID_{start_time.strftime("%Y%m%d_%H_%M")}' #ingestion process ID
        logger.info(f"Log ID: {log_id}")

        insert_pipeline_log(cur, log_id, start_time)

        try:
            logger.info('Job Started')
            loader = FileLoader()
            loader._retrieve_localdir_from_syncfile()
            end_time = datetime.now()

            q_check_count = """
                SELECT COUNT(*) 
                FROM file_ingestion_log 
                WHERE load_start_time >= %s AND load_end_time <= %s
            """
            cur.execute(q_check_count, (start_time, end_time))
            records_processed = cur.fetchone()[0]

            update_pipeline_log(cur, log_id, end_time, 'Job Passed', 'No Errors', records_processed)
            logger.info('Job was run Successfully')

        except Exception as e:
            error_msg = str(e)
            end_time = datetime.now()
            update_pipeline_log(cur, log_id, end_time, 'Job Failed', error_msg)
            logger.exception('Job run was unsuccessfully')

    except psycopg2.Error as e:
        logger.exception(f"Error connecting to PostgreSQL database: {e}")
        raise

    finally:
        # Close cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
