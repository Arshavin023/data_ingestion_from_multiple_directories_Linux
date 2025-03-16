SELECT 'file_ingest_process',
SUM(CASE WHEN processed =2 THEN 1 ELSE 0 END) processed_count,
SUM(CASE WHEN processed =0 THEN 1 ELSE 0 END) just_uploaded,
SUM(CASE WHEN processed =-1 THEN 1 ELSE 0 END) decryption_queue,
SUM(CASE WHEN processed =1 THEN 1 ELSE 0 END) decrypted_complete
FROM public.sync_file
WHERE modified_date >= '2025-01-01'
AND NOT (decrypted_file_name ILIKE ANY (
ARRAY['prep_eligibility_%','prep_clinic_%','mhpss_confirmation_%'
	'pmtct_anc_%','dsd_devolvement%','hiv_art_clinical%'])
)
-- )
GROUP BY 1
union all
SELECT 'dsd_ingest_process',
SUM(CASE WHEN processed =2 THEN 1 ELSE 0 END) processed_count,
SUM(CASE WHEN processed =0 THEN 1 ELSE 0 END) just_uploaded,
SUM(CASE WHEN processed =-1 THEN 1 ELSE 0 END) decryption_queue,
SUM(CASE WHEN processed =1 THEN 1 ELSE 0 END) decrypted_complete
FROM public.sync_file
WHERE modified_date >= '2025-01-01'
AND (decrypted_file_name ILIKE ANY (
ARRAY['prep_eligibility_%','prep_clinic_%',
	'pmtct_anc_%','dsd_devolvement%','hiv_art_clinical%'])
)
GROUP BY 1;


SELECT REGEXP_REPLACE(file_name, '_[0-9]+.*|\\.json', '') table_name, 'UNPROCESSED', COUNT(file_name)
FROM sync_file
WHERE processed=1 AND modified_date >= '2025-01-01'
AND NOT (decrypted_file_name ILIKE ANY(ARRAY[
'mhpss_confirmation_%', 'prep_eligibility_%', 'prep_clinic_%',
'pmtct_anc_%', 'dsd_devolvement_%', 'hiv_art_clinical_%'
]))
GROUP BY 1, 2;

SELECT *, (end_time-start_time) time_taken
FROM public.batch_stg_table_logs
WHERE status='PROCESSED'
ORDER BY table_name_count DESC;

SELECT *
FROM public.batch_stg_table_logs
WHERE status != 'PROCESSED'
ORDER BY table_name_count DESC;

SELECT MIN(start_time) min_start, MAX(end_time) max_end
FROM public.batch_stg_table_logs
-- WHERE status != 'PROCESSED'
ORDER BY table_name_count DESC;

SELECT DISTINCT facility_id
FROM sync_file
WHERE ingest_end_time BETWEEN '2025-01-03 19:10:00' AND '2025-01-03 19:22:00'
AND file_name ILIKE 'patient_person_%'
-- AND processed=-2
LIMIT 100;

SELECT * FROM sync_file
WHERE file_name='patient_person_0_20250103141949.json'

UPDATE sync_file
SET processed=0
WHERE file_name ILIKE 'hts_risk_stratification%'
AND modified_date >= '2025-01-01'
AND processed != 0;

, ingest_error_message=null,
ingest_file_name=null,ingest_start_time=null,
ingest_status_check=null,json_rec_count=null
WHERE ingest_start_time >= '2025-01-03 19:10:00'
AND ingest_end_time <= '2025-01-03 19:23:00';

SELECT * FROM sync_file 
WHERE file_name ILIKE 'hts_risk_stratification%' 
ORDER BY modified_date DESC
LIMIT 100;
SELECT *
FROM public.sync_file
WHERE modified_date >= '2025-01-01'
AND NOT (decrypted_file_name ILIKE ANY (
ARRAY['prep_eligibility_%','prep_clinic_%','mhpss_confirmation_%'
	'pmtct_anc_%','dsd_devolvement%','hiv_art_clinical%'])
)
AND processed=1