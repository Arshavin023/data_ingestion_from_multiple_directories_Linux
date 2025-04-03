SELECT table_name, 
       pg_size_pretty(pg_total_relation_size((table_schema || '.' || table_name)::regclass)) AS size,
       pg_total_relation_size((table_schema || '.' || table_name)::regclass) AS size_bytes
FROM information_schema.tables
WHERE table_schema = 'public' 
  AND table_name ILIKE 'stg_%' 
  AND pg_total_relation_size((table_schema || '.' || table_name)::regclass) > 0
  AND table_name !~ '_[0-9]+$'
  AND table_name NOT ILIKE '%_base_%'
ORDER BY size_bytes DESC
LIMIT 100;


