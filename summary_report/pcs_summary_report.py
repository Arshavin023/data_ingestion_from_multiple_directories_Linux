import psycopg2

def _db_connect_filedb():
    db_params = {
    'host': 'localhost',
    'database': 'filedb',
    'user': 'lamisplus',
    'password': '37EpE&U&H?',
    'port': '5432',
        }

        # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    return conn


def _run_summary_report_pipeline():

    conn = _db_connect_filedb()

    print('connection created successfully')

    cur=conn.cursor()

    extract_report = """INSERT INTO process_summary_report
    SELECT count(id) Total_Files,
    SUM(CASE WHEN processed =2 THEN 1 ELSE 0 END) processed_count,
    SUM(CASE WHEN processed =0 THEN 1 ELSE 0 END) just_uploaded,
    SUM(CASE WHEN processed =-1 THEN 1 ELSE 0 END) decryption_queue,
    SUM(CASE WHEN processed =1 THEN 1 ELSE 0 END) decrypted_complete,
    SUM(CASE WHEN processed =-2 AND ingest_status_check is null THEN 1 ELSE 0 END) real_decryption_fails,
    SUM(CASE WHEN processed =-2 AND ingest_status_check is not null THEN 1 ELSE 0 END) ingestion_fails,
    SUM(CASE WHEN processed =-2 THEN 1 ELSE 0 END) fails, CURRENT_TIMESTAMP check_data
    FROM public.sync_file
    WHERE create_date >= '2024-03-21' and decrypted_file_name like '%dsd_devolvement%'
    UNION ALL
    SELECT count(id) Total_Files,
    SUM(CASE WHEN processed =2 THEN 1 ELSE 0 END) processed_count,
    SUM(CASE WHEN processed =0 THEN 1 ELSE 0 END) just_uploaded,
    SUM(CASE WHEN processed =-1 THEN 1 ELSE 0 END) decryption_queue,
    SUM(CASE WHEN processed =1 THEN 1 ELSE 0 END) decrypted_complete,
    SUM(CASE WHEN processed =-2 AND ingest_status_check is null THEN 1 ELSE 0 END) real_decryption_fails,
    SUM(CASE WHEN processed =-2 AND ingest_status_check is not null THEN 1 ELSE 0 END) ingestion_fails,
    SUM(CASE WHEN processed =-2 THEN 1 ELSE 0 END) fails, CURRENT_TIMESTAMP check_data
    FROM public.sync_file
    WHERE create_date >= '2024-03-21' and decrypted_file_name not like '%dsd_devolvement%' """

    cur.execute(extract_report)

    print('report extracted and inserted successfully')

    conn.commit()
    cur.close()

print('running pipeline')
_run_summary_report_pipeline()