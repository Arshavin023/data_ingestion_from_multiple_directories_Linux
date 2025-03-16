with duplicate_check as (
select file_name, facility_id,create_date, modified_date, ingest_end_time,processed,
	row_number() over (partition by file_name, facility_id order by modified_date desc) row_num
from public.sync_file
where facility_id in ('z6APuLTiHAX','FQ0Jc3pO5Op','DPA6BxE9pzQ','XyFDmlevgCu','RxLmSTf6Kpl',
'eJDSKTK7sO8','FxDuhc7a7mg','Ufkohku0hkU','rSLoHKc0nGd','sMllNyKolZf','gW4HoZwOJ9J','IiIYrVsq9rk',
'GBFvMIKcqNH','bQRrBX1dImc','sCJI7AumkSI','y9g73y2KvTz','x28h6GM4JaJ','zYDjpVGyR6R','Xb8UqZSE3u2',
'X1b3ELB9LTX','rSmFsJpZIM9','IU4lXhgmRYl','Jg82rXVgIjp','hxMSGzxlJoy','BVEHIFI2Fea','TqP0yUIXyDJ',
'konOEZuZo9H','sdO42TShMDc','mxeORAzNzal','ixltW74M2ch','MbdvhJg5Jag','eOrrK6aBIrt','BNvTkO5uvaN',
'NAGd5Q25uMa','QYEUyQCxByE','SoQeEge5A3R','NIulfQauxlQ','bz9lwL7ta7W','JZL6IMoe5G3','MuYD8OawdzS',
'wZTPrbae9ie','GU7GAjOSjpt','n2XQ6lwoh1F','hJnEYRxvSz2','nWMaBEIKXDv','f0J277xHATh','cmkm8UQpvWk')
and ingest_error_message not ilike '%UnicodeDecodeError - File is corrupted and unreadable, kindly regenerate and re-upload%'
--and ingest_error_message not ilike '%ProgrammingError - (UndefinedColumn) column%'
and processed in (2,-2)
and ingest_end_time is null
order by modified_date desc;
) select * from duplicate_check where processed = 2 and row_num > 1;


UPDATE sync_file AS a
SET ingest_error_message = 'No errors'
FROM public.sync_file AS b
WHERE a.file_name = b.file_name AND a.processed != b.processed AND a.id != b.id AND b.processed = 2;

-- delete duplicates
WITH duplicate_check AS (
    SELECT 
        file_name, 
        facility_id, 
        processed, 
        ingest_error_message,
        ROW_NUMBER() OVER (PARTITION BY file_name, facility_id ORDER BY modified_date DESC) AS row_num
    FROM 
        public.sync_file
    WHERE 
        facility_id IN ('z6APuLTiHAX','FQ0Jc3pO5Op','DPA6BxE9pzQ','XyFDmlevgCu','RxLmSTf6Kpl',
                        'eJDSKTK7sO8','FxDuhc7a7mg','Ufkohku0hkU','rSLoHKc0nGd','sMllNyKolZf',
                        'gW4HoZwOJ9J','IiIYrVsq9rk','GBFvMIKcqNH','bQRrBX1dImc','sCJI7AumkSI',
                        'y9g73y2KvTz','x28h6GM4JaJ','zYDjpVGyR6R','Xb8UqZSE3u2','X1b3ELB9LTX',
                        'rSmFsJpZIM9','IU4lXhgmRYl','Jg82rXVgIjp','hxMSGzxlJoy','BVEHIFI2Fea',
                        'TqP0yUIXyDJ','konOEZuZo9H','sdO42TShMDc','mxeORAzNzal','ixltW74M2ch',
                        'MbdvhJg5Jag','eOrrK6aBIrt','BNvTkO5uvaN','NAGd5Q25uMa','QYEUyQCxByE',
                        'SoQeEge5A3R','NIulfQauxlQ','bz9lwL7ta7W','JZL6IMoe5G3','MuYD8OawdzS',
                        'wZTPrbae9ie','GU7GAjOSjpt','n2XQ6lwoh1F','hJnEYRxvSz2','nWMaBEIKXDv',
                        'f0J277xHATh','cmkm8UQpvWk')
        AND ingest_error_message NOT ILIKE '%UnicodeDecodeError - File is corrupted and unreadable, kindly regenerate and re-upload%'
        AND processed = 2
)
DELETE FROM public.sync_file
WHERE (file_name, facility_id) IN (
    SELECT file_name, facility_id
    FROM duplicate_check
    WHERE row_num > 1
);

rollback;