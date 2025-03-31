import psycopg2
from datetime import datetime
from multithread_file_loader import FileLoader
import configparser
import concurrent.futures
from src import logger
from sqlalchemy import create_engine

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


def db_connect(database:str):
    '''
    Establishes a connection to the specified PostgreSQL database.
    Parameters:
    - database (str): The name of the database to connect to.
    Returns:
    - conn (psycopg2.connection): The connection object.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine object.
    Raises:
    - Exception: If connection to the database fails.
    '''
    db_params = {'host': db_config['stg_host'], 'database': database, 'user': db_config['stg_username'],
                 'password': db_config['stg_password'],'port': db_config['stg_port'],}
    try:
        conn = psycopg2.connect(**db_params)
        engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        
        return [conn, engine]
    
    except Exception as e:
        logger.exception(e)
        raise e
            

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
    insert_query = """
                   INSERT INTO batch_facility_processing(facility_id,status,file_count)
                   SELECT facility_id,'UNPROCESSED' status, COUNT(1) file_count
                   FROM sync_file
                   WHERE processed=%s AND modified_date >= %s
                   GROUP BY 1,2
                   """
    PROCESSED = 1
    MODIFIED_DATE = '2025-01-01'
    try:
        with db_connect('filedb')[0] as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, (PROCESSED, MODIFIED_DATE))
                conn.commit()
                logger.info('Facility batch file uploads inserted into batch_facility_processing')
                
    
    except psycopg2.Error as e:
        logger.exception("Error connecting to PostgreSQL database and inserting records", exc_info=True)

def update_facility_uploads(cur_status,start_time,end_time,error_message,facility_id,batch_id, prev_status):
    update_query = """
                   UPDATE batch_facility_processing
                   SET status=%s, start_time=%s, end_time=%s, error_message=%s
                   WHERE facility_id=%s AND id=%s AND status=%s
                   """
    try:
        with db_connect('filedb')[0] as conn:
            with conn.cursor() as cur:
                cur.execute(update_query, (cur_status, start_time, end_time, error_message, 
                                            facility_id, batch_id, prev_status))
                conn.commit()
                logger.info(f'files updated for {facility_id} in batch_facility_processing')
    
    except psycopg2.Error as e:
        logger.exception("Error connecting to PostgreSQL database and updating records", exc_info=True)
    
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

def main():
    db_params = {
        'host': db_config['stg_host'],
        'database': db_config['stg_database_name'],
        'user': db_config['stg_username'],
        'password': db_config['stg_password'],
        'port': db_config['stg_port'],
    }
    
    insert_facility_uploads()
    
    filedb_conn=db_connect('filedb')[0]
    filedb_cur = filedb_conn.cursor()
            
    filedb_cur.execute("""
        SELECT id, facility_id, file_count 
        FROM batch_facility_processing 
        WHERE status='UNPROCESSED'
                """)
    facilities = filedb_cur.fetchall()  # Fetch records from DB

    try:
        with db_connect('lamisplus_staging_dwh')[0] as conn:
            with conn.cursor() as cur:
                start_time = datetime.now() 
                log_id = f'IPID_{start_time.strftime("%Y%m%d_%H_%M")}' #ingestion process ID
                logger.info(log_id)
                
                insert_pipeline_log(cur, log_id, start_time)
                
                try:
                    logger.info('Job Started')
                    if facilities:
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                           executor.map(create_single_instance, facilities)
                           
                    logger.info('processing of facilities done')
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
    
    except psycopg2.Error as e:
        logger.info("Error connecting to PostgreSQL database:", e)

if __name__ == '__main__':
    main()
