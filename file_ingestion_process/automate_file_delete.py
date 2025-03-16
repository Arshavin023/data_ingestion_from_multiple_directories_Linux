import json 
import psycopg2
import os
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine, JSON, Integer, String, Float, DateTime, Boolean
from sqlalchemy.dialects.postgresql import JSONB
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

class FileDelete:
    def __init__(self):
        self.facility_id = None
        self.syncfile_entryID = None
        self.demo_path = '/home/lamisplus/server/temp'
        self.count_of_df = 0
        self.delete_end_time = None
        self.delete_start_time = None
        
    def _db_connect(self, database:str):
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
            
    def _insert_into_log(self,tableName, fileName, facilityId):
            conn= self._db_connect('filedb')[0]
            cur = conn.cursor()
            deletion_start_time = self.delete_start_time
            deletion_status_check = 'processing'
            table_name = tableName
            file_name = fileName
            facility_id = facilityId

            insert_query = """insert into file_deletion_log 
            (deletion_start_time, deletion_status_check, table_name, file_name, facility_id) 
            values ('{}','{}','{}','{}', '{}') RETURNING id""".format(deletion_start_time, deletion_status_check, table_name, file_name, facility_id)

            cur.execute(insert_query)
            #logger.info("inserted successfully")
            log_id =  cur.fetchall()[0][0]
            conn.commit()
            cur.close()
            return log_id

    def count_rows_in_json_file(file_path):
        try:
            with open(file_path, 'r') as file:
                try:
                    data = json.load(file)
                    num_rows = len(data)
                    return num_rows

                except json.JSONDecodeError as e:
                    #logger.info(f"Error decoding JSON file {os.path.basename(file_path)}: {str(e)}")
                    logger.exception(e)
                    return 0

        except Exception as e:
            logger.exception(e)
            #logger.info(f"No such file or directory {os.path.basename(file_path)}: {str(e)}")
            return 0

    def _update_log(self, id, proc_status, file_name, tab_count, error_msg):
            conn=self._db_connect('filedb')[0]
            cur = conn.cursor()
            deletion_end_time = self.delete_end_time
            deletion_status_check = proc_status

            update_query = """UPDATE file_deletion_log 
                            SET deletion_end_time =  %s,
                            deletion_status_check =  %s, json_rec_count =  %s, error_message = %s
                            WHERE id =  %s
                            """

            cur.execute(update_query,(deletion_end_time, deletion_status_check, 
                                      tab_count, error_msg, id))
            conn.commit()
            cur.close()
            logger.info(f'{file_name} log updated successfully')

    def _process_derive_tablename(self, file_path):
            filename = os.path.basename(file_path)
            parts = filename.split('_')
            non_digit_parts = [part for part in parts if not part.isdigit() and part != 'decrypted.json']
            result=[]
            result.append('_'.join(non_digit_parts))
            check_path = result[0]
            #logger.info(check_path)
            return check_path

         
    def delete_ingested_decrypted_files(self):
        try:
            conn = self._db_connect('filedb')[0]
            cur = conn.cursor()
            retrieve_query = """SELECT sf.facility_id, sf.ingest_file_name
                                FROM public.sync_file sf
                                WHERE sf.processed IN (2,-2) 
                                AND sf.modified_date <= CURRENT_DATE - 30 
                                AND sf.ingest_end_time IS NOT NULL 
				AND sf.ingest_file_name IS NOT NULL
                                AND NOT EXISTS (
                                    SELECT 1
                                    FROM public.file_deletion_log fdl
                                    WHERE fdl.file_name=sf.ingest_file_name
                                    AND fdl.deletion_status_check in ('success','failed')
				    AND fdl.file_name ILIKE '%_decrypted%')
                                LIMIT 15000
                                """
            cur.execute(retrieve_query)

            # Fetch all file associated data from sync_file
            files = cur.fetchall()
            for file in files:
                self.delete_start_time = datetime.now()
                self.facility_id = file[0]
                decryptedjson_file_name = file[1]
                local_dir = os.path.join(self.demo_path,self.facility_id,decryptedjson_file_name)
                filelog_id = self._insert_into_log(self._process_derive_tablename(local_dir), decryptedjson_file_name, self.facility_id)
                try:
                    if os.path.exists(local_dir):
                        logger.info(f"File: {local_dir} exists")
                        os.remove(local_dir)
                        logger.info(f"File deleted: {local_dir}")
                        self.delete_end_time = datetime.now()
                        self._update_log(filelog_id,'success',decryptedjson_file_name,self.count_of_df, 'no errors')
                    else:
                        logger.error(f"File {local_dir} not found")
                        self.delete_end_time = datetime.now()
                        self._update_log(filelog_id,'failed',decryptedjson_file_name,self.count_of_df,'file not found')
                except PermissionError as e:
                    logger.error(f"Permission error deleting {local_dir}: {str(e)}")
                    self.delete_end_time=datetime.now()
                    self._update_log(filelog_id,'failed',decryptedjson_file_name,self.count_of_df,f"Permission error: {str(e)}")
                logger.info('----------------------------------------------------------------------------------------------')
                

            # Commit the changes and close the connection
            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            logger.info(f"Error: {str(e)}")
        

    def delete_encrypted_files(self):

        try:
            conn = self._db_connect('filedb')[0]
            cur = conn.cursor()
            retrieve_query = """SELECT sf.facility_id, sf.file_name
                                FROM public.sync_file sf
                                WHERE sf.processed IN (2, -2)
                                  AND sf.modified_date >= '2024-09-01'
                                  AND sf.ingest_end_time IS NOT NULL
                                  AND sf.file_name IS NOT NULL
                                  AND NOT EXISTS (
                                    SELECT 1
                                    FROM public.file_deletion_log fdl
                                    WHERE fdl.file_name = sf.file_name
                                      AND fdl.deletion_status_check IN ('success', 'failed')
                                      AND fdl.file_name NOT ILIKE '%_decrypted%')
                                LIMIT 13000
                                """
            cur.execute(retrieve_query)

            # Fetch all file associated data from sync_file
            files = cur.fetchall()
            for file in files:
                self.delete_start_time = datetime.now()
                self.facility_id = file[0]
                encryptedjson_file_name = file[1]
                local_dir = os.path.join(self.demo_path,self.facility_id,encryptedjson_file_name)
                filelog_id = self._insert_into_log(self._process_derive_tablename(local_dir), encryptedjson_file_name, self.facility_id)
                try:
                    if os.path.exists(local_dir):
                        logger.info(f"File: {local_dir} exists")
                        os.remove(local_dir)
                        logger.info(f"File deleted: {local_dir}")
                        self.delete_end_time = datetime.now()
                        self._update_log(filelog_id,'success',encryptedjson_file_name,self.count_of_df, 'no errors')
                    else:
                        logger.error(f"File {local_dir} not found")
                        self.delete_end_time = datetime.now()
                        self._update_log(filelog_id,'failed',encryptedjson_file_name,self.count_of_df,'file not found')
                except PermissionError as e:
                    logger.error(f"Permission error deleting {local_dir}: {str(e)}")
                    self.delete_end_time=datetime.now()
                    self._update_log(filelog_id,'failed',encryptedjson_file_name,self.count_of_df,f"Permission error: {str(e)}")
                logger.info('----------------------------------------------------------------------------------------------')

            # Commit the changes and close the connection
            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            logger.info(f"Error: {str(e)}")
