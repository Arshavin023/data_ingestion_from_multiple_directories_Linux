from datetime import datetime
import psycopg2
from src import logger
import configparser
from sqlalchemy import create_engine
import concurrent.futures
from database_connection.db_connect import connect_to_db

def create_single_instance(staging_table:str):
    stg_conn = connect_to_db.connect('lamisplus_staging_dwh')[0]
    stg_cur = stg_conn.cursor()
    delete_query_template = "CALL proc_delete_stg_records(%s)"
    try:
        logger.info(f"Deletion of ods_migrated records from {staging_table[0]} table started")
        stg_cur.execute(delete_query_template, (staging_table[0],))
        stg_conn.commit()
        logger.info(f"Deletion of ods_migrated records from {staging_table[0]} table completed")

    except Exception as e:
        stg_conn.rollback()
        logger.exception(f"Error processing {staging_table}: {str(e)}")

    finally:
        stg_cur.close()
        stg_conn.close()  # Close connection after execution


def main():
    conn = connect_to_db.connect('lamisplus_staging_dwh')[0]
    cur = conn.cursor()
    retrieve_stg_tables = """
        SELECT cast(table_name as varchar) table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name ILIKE 'stg_%' and table_name NOT ILIKE '%_bad_dates' 
        AND table_name IN (
            'stg_hiv_art_pharmacy_regimens','stg_hiv_eac_out_come','stg_base_organisation_unit','stg_base_application_codeset',
            'stg_base_organisation_unit_identifier','stg_biometric','stg_case_manager','stg_case_manager_patients',
            'stg_dsd_devolvement','stg_hiv_art_clinical','stg_hiv_art_pharmacy','stg_hiv_eac',
            'stg_hiv_eac_session','stg_hiv_enrollment','stg_hiv_observation','stg_hiv_regimen',
            'stg_hiv_regimen_resolver','stg_hiv_regimen_type','stg_hiv_status_tracker','stg_hts_client',
            'stg_hts_index_elicitation','stg_hts_risk_stratification','stg_laboratory_labtest','stg_laboratory_test',
            'stg_laboratory_order','stg_laboratory_result','stg_laboratory_sample','stg_patient_encounter',
            'stg_patient_person','stg_patient_visit','stg_pmtct_anc','stg_pmtct_delivery',
            'stg_pmtct_enrollment','stg_pmtct_infant_arv','stg_pmtct_infant_information','stg_pmtct_infant_mother_art',
            'stg_pmtct_infant_pcr','stg_pmtct_infant_rapid_antibody','stg_pmtct_infant_visit','stg_pmtct_mother_visitation',
            'stg_prep_clinic','stg_prep_eligibility','stg_prep_enrollment','stg_prep_interruption',
            'stg_triage_vital_sign'
            )
        """
                
    cur.execute(retrieve_stg_tables)
    staging_tables = cur.fetchall()

    # Connect to the PostgreSQL database
    try:
        start_time = datetime.now() 
        log_id = f'DPID_{start_time.strftime("%Y%m%d_%H_%M")}' #deletion process ID
        logger.info(log_id)
        logger.info('Job Started')
        logger.info('Deletion of records with Y processed status on stg_monitoring table started')
        if staging_tables:
            batch_size = 10  # Adjust based on your system capacity
            for i in range(0, len(staging_tables), batch_size):
                batch = staging_tables[i:i + batch_size]
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.map(create_single_instance, batch)
        conn.commit()
        logger.info('Deletion of records with Y processed status on stg_monitoring table completed')

    except Exception as e:
        conn.rollback()
        logger.exception(f"Error deleting staging tables records: {str(e)}")

    cur.close()

if __name__ == '__main__':
    main()
