from automate_file_delete import FileDelete
from datetime import datetime
import psycopg2
from src import logger
import configparser
from database_connection.db_connect import connect_to_db

def main():
    # Connect to the PostgreSQL database
    conn = connect_to_db.connect('lamisplus_staging_dwh')[0]
    cur = conn.cursor()

    conn2 = connect_to_db.connect('filedb')[0]
    cur2 = conn2.cursor()

    start_time = datetime.now()
    log_id = f'DPID_{start_time.strftime("%Y%m%d_%H_%M")}'
    logger.info(log_id)

    insert_pipeline_query = """insert into file_ingestion_pipeline_log (log_id, start_time, status, process_type) 
    VALUES ('{}','{}','{}', '{}')""".format(log_id, start_time, 'Job Started', 'file deletion')

    cur.execute(insert_pipeline_query)
    conn.commit()
    try:
        logger.info('Deletion of json files started')
        delete_start_time = datetime.now()
        deletion_encrypted = FileDelete()
        deletion_encrypted.delete_encrypted_files()
        logger.info('Deletion of json files completed')
        delete_end_time = datetime.now()
        q_check_count = """select count(*) from file_deletion_log 
        where  
        deletion_start_time >=  %s and deletion_end_time <=  %s
        """
        cur2.execute(q_check_count, (delete_start_time, delete_end_time))
        records_processed = cur2.fetchall()[0]
        update_pipeline_query = """update file_ingestion_pipeline_log set end_time=  %s
        , status =  %s, error_message=  %s, records_processed=  %s
        where log_id =  %s
        """ 
        cur.execute(update_pipeline_query, (delete_end_time, 'Job Passed', 'No Errors', records_processed, log_id))
        conn.commit()
        logger.info('Deletion Job for decrypted files was run successfully completed')

    except Exception as e:
        error_msg =str(e)
        end_time = datetime.now()
        logger.error(error_msg) 
        conn.commit()
        
    
    conn.commit()
    cur.close()
    conn2.commit()

if __name__ == '__main__':
    main()
