

## Quick Summary Overview of Ingestion Process
```
select count(id) Total_Files, 
SUM(CASE WHEN processed =2 THEN 1 ELSE 0 END) processed_count,
SUM(CASE WHEN processed =0 THEN 1 ELSE 0 END) just_uploaded,
SUM(CASE WHEN processed =-1 THEN 1 ELSE 0 END) decryption_queue,
SUM(CASE WHEN processed =1 THEN 1 ELSE 0 END) decrypted_complete,
SUM(CASE WHEN processed =-2 AND ingest_status_check is null THEN 1 ELSE 0 END) real_decryption_fails,
SUM(CASE WHEN processed =-2 AND ingest_status_check is not null THEN 1 ELSE 0 END) ingestion_fails,
min(create_date) first_upload_date,
max(create_date) latest_upload_date,
SUM(CASE WHEN processed =-2 THEN 1 ELSE 0 END) fails, CURRENT_TIMESTAMP check_data
FROM sync_file
```

## Quick Summary Overview of Ingestion Process for a Specific Date Period

```
select count(id) Total_Files, 
SUM(CASE WHEN processed =2 THEN 1 ELSE 0 END) processed_count,
SUM(CASE WHEN processed =0 THEN 1 ELSE 0 END) just_uploaded,
SUM(CASE WHEN processed =-1 THEN 1 ELSE 0 END) decryption_queue,
SUM(CASE WHEN processed =1 THEN 1 ELSE 0 END) decrypted_complete,
SUM(CASE WHEN processed =-2 AND ingest_status_check is null THEN 1 ELSE 0 END) real_decryption_fails,
SUM(CASE WHEN processed =-2 AND ingest_status_check is not null THEN 1 ELSE 0 END) ingestion_fails,
min(create_date) first_upload_date,
max(create_date) latest_upload_date,
SUM(CASE WHEN processed =-2 THEN 1 ELSE 0 END) fails, CURRENT_TIMESTAMP check_data
FROM sync_file 
where DATE_TRUNC('day', create_date) >= '2024-01-31'
```


## SQL Script to check report for Specific facility(ies) and specific tables
```
select REGEXP_REPLACE(TRIM(decrypted_file_name), '_\d+_\d+\.json$', '' ) table_name , facility_id, count(id) Total_Files,
SUM(CASE WHEN processed =2 THEN 1 ELSE 0 END) processed_count,
SUM(CASE WHEN processed =0 THEN 1 ELSE 0 END) just_uploaded,
SUM(CASE WHEN processed =-1 THEN 1 ELSE 0 END) decryption_queue,
SUM(CASE WHEN processed =1 THEN 1 ELSE 0 END) decrypted_complete,
SUM(CASE WHEN processed =-2 AND ingest_status_check is null THEN 1 ELSE 0 END) real_decryption_fails,
SUM(CASE WHEN processed =-2 AND ingest_status_check is not null THEN 1 ELSE 0 END) ingestion_fails,
min(create_date) first_date_uploaded,
max(create_date) most_recent_upload,
SUM(CASE WHEN processed =-2 THEN 1 ELSE 0 END) fails, CURRENT_TIMESTAMP check_data
FROM sync_file
where facility_id in ('QpIjNJI8vUS')
and REGEXP_REPLACE(TRIM(decrypted_file_name), '_\d+_\d+\.json$', '' ) in ('patient_person', 'hiv_art_pharmacy')
group by REGEXP_REPLACE(TRIM(decrypted_file_name), '_\d+_\d+\.json$', '' ),facility_id
```

## Check Facilities wrongly mapped
```
with _cte_cte as (
select ip_name, a.facility_id, b.facility_name, REGEXP_REPLACE(TRIM(decrypted_file_name), '_\d+_\d+\.json$', '' ) tab_name, count(a.id) Total_Files, 
SUM(CASE WHEN processed =2 THEN 1 ELSE 0 END) processed_count,
SUM(CASE WHEN processed =0 THEN 1 ELSE 0 END) just_uploaded,
SUM(CASE WHEN processed =-1 THEN 1 ELSE 0 END) decryption_queue,
SUM(CASE WHEN processed =1 THEN 1 ELSE 0 END) decrypted_complete,
SUM(CASE WHEN processed =-2 AND ingest_status_check is null THEN 1 ELSE 0 END) real_decryption_fails,
SUM(CASE WHEN processed =-2 AND ingest_status_check is not null THEN 1 ELSE 0 END) ingestion_fails,
min(create_date) first_upload_date,
max(create_date) latest_upload_date,
SUM(CASE WHEN processed =-2 THEN 1 ELSE 0 END) fails, CURRENT_TIMESTAMP check_data
FROM sync_file a
LEFT JOIN central_partner_mapping b
ON a.facility_id = b.datim_id
where 
--DATE_TRUNC('day',create_date) >= '2024-01-30' and 
REGEXP_REPLACE(TRIM(decrypted_file_name), '_\d+_\d+\.json$', '' ) not like 'meta_data%'
group by ip_name, a.facility_id, b.facility_name,REGEXP_REPLACE(TRIM(decrypted_file_name), '_\d+_\d+\.json$', '' )
having count(a.id) <> SUM(CASE WHEN processed =2 THEN 1 ELSE 0 END) and SUM(CASE WHEN processed =1 THEN 1 ELSE 0 END) = 0)

select distinct facility_id from _cte_cte
where ip_name is null
```

## Check 24 hrs processing Exceptions 
i.e files that have been on the decryption queue for more than 24 hours. 

Run update block to fix them and revert them to zero for re-processing

```
with _cte_24hours_exceptions as (select CASE WHEN current_timestamp - create_date >= interval '24 hours'
THEN 'exception' ELSE 'compliant' END AS check_compliance, * from sync_file
where processed = -1)

select * from _cte_24hours_exceptions;

--select count(*) from _cte_24hours_exceptions where check_compliance = 'exception' --1047

update sync_file
set processed = 0
where id in (select id from _cte_24hours_exceptions where check_compliance = 'exception');
```

## PgSQL Anonymous block to truncate all tables in DWH 

```
DO $$	
DECLARE
    table_to_drop TEXT;
BEGIN
    -- Loop through the rows in your table
    FOR table_to_drop IN (SELECT distinct table_name
FROM information_schema.tables
where table_catalog = 'lamisplus_staging_dwh'
and table_schema = 'public' and table_name like 'stg_%' and table_name <> 'stg_monitoring')
    LOOP
        -- Construct the dynamic SQL statement
        EXECUTE 'TRUNCATE TABLE ' || table_to_drop;
    END LOOP;
	truncate public.file_ingestion_log;
	truncate stg_monitoring;
END $$;
```