import psycopg2
import os
from datetime import datetime
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

class StgRecordDelete:
    def __init__(self):
        self.facility_id = None
        self.syncfile_entryID = None
        self.demo_path = '/home/lamisplus/server/temp'
        self.count_of_df = 0
        self.delete_end_time = None
        self.delete_start_time = None

    def _db_connect_lamisplus_staging_dwh(self):
        db_params = {
        'host': db_config['stg_host'],
        'database': 'lamisplus_staging_dwh',
        'user': db_config['stg_username'],
        'password': db_config['stg_password'],
        'port': db_config['stg_port'],}

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        return conn

    def delete_staging_table_records(self):
        try:
            conn = self._db_connect_lamisplus_staging_dwh()
            cur = conn.cursor()
            
            retrieve_query = """
                    SELECT cast(table_name as varchar) table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name ILIKE 'stg_%' and table_name NOT ILIKE '%_bad_dates' AND table_name !='stg_monitoring'
                    ORDER BY pg_total_relation_size(quote_ident(table_name)) desc
                    LIMIT 100                """
                
            delete_query_template = "CALL proc_delete_stg_records(%s)"
            
            cur.execute(retrieve_query)
            staging_tables = cur.fetchall()
                
            for stg in staging_tables:
                try:
                    logger.info(f"Deletion of ods_migrated records from {stg[0]} table started")
                    cur.execute(delete_query_template, (stg[0],))
                    conn.commit()
                    logger.info(f"Deletion of ods_migrated records from {stg[0]} table completed")

                except Exception as e:
                    conn.rollback()
                    logger.exception(f"Error processing {stg[0]}: {str(e)}")

            cur.close()
        
        except psycopg2.Error as e:
            logger.error(f"Database error: {str(e)}")
        
        finally:
            if conn:
                conn.close()
