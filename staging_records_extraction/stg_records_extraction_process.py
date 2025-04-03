from file_ingestion_process.staging_records_extraction.automate_table_extraction import StgRecordExtraction
from datetime import datetime
import psycopg2
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

def main():
    db_params = {
    'host': db_config['stg_host'],
    'database': 'lamisplus_staging_dwh',
    'user': db_config['stg_username'],
    'password': db_config['stg_password'],
    'port': db_config['stg_port'],}
    
    db_params2 = {
        'host': db_config['stg_host'],
        'database': 'filedb',
        'user': db_config['stg_username'],
        'password': db_config['stg_password'],
        'port': db_config['stg_port'],}
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    conn2 = psycopg2.connect(**db_params2)
    cur2 = conn2.cursor()

    start_time = datetime.now() 
    log_id = f'DPID_{start_time.strftime("%Y%m%d_%H_%M")}' #deletion process ID
    logger.info(log_id)

    try:
        logger.info('Job Started')
        logger.info('Exraction of bad dates records from staging tables started')
        deleting_records = StgRecordExtraction()
        deleting_records.extract_bad_dates_tables()
        conn.commit()
        logger.info('Exraction of bad dates records from staging tables completed')
        
    except Exception as e:
        error_msg =str(e)
        print(error_msg)
        conn.commit()
        
    
    conn.commit()
    cur.close()
    conn2.commit()

if __name__ == '__main__':
    main()
