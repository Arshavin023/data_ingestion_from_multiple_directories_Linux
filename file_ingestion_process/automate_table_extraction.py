import psycopg2
import os
import pandas as pd
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

class StgRecordExtraction:
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
            'port': db_config['stg_port'],
        }

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        return conn

    def extract_bad_dates_tables(self):
        conn = None
        try:
            conn = self._db_connect_lamisplus_staging_dwh()
            cur = conn.cursor()
            retrieve_query = """
                SELECT distinct table_name 
                FROM information_schema.columns
                WHERE table_name ILIKE '%_bad_dates'
            """
            cur.execute(retrieve_query)
            bad_dates_tables = cur.fetchall()
                
            for stg in bad_dates_tables:
                try:
                    table = stg[0]
                    logger.info(f"Selecting from {table} table started")                    
                    select_query = f"SELECT * FROM {table}"
                    df_bad_date = pd.read_sql_query(select_query, conn)  # Use conn here
                    df_bad_date.to_csv(f'/home/uche/csv_tables/{table}.csv', index=False)
                    logger.info(f"extraction of {table} table completed")

                except Exception as e:
                    conn.rollback()
                    logger.exception(f"Error processing {table}: {str(e)}")

            cur.close()
        
        except psycopg2.Error as e:
            logger.error(f"Database error: {str(e)}")
        
        finally:
            if conn is not None:
                conn.close()
