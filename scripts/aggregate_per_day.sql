select 
    refresh_date,
    count(*) as count_daily
from `guillermo-sanchez-sndbx-u.guille_dev_staging.stg_international_top_rising_terms_AT` 
group by 1
order by 1 desc