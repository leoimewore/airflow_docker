{{
    config(
        materialized='view'
    )
}}


WITH persons_data AS (
    SELECT  
        *,
        {{fill_safety_equipment('safety_equipment')}} as safety_equipment_updated,
        extract(month from crash_date) AS crash_month_number,
        extract(year from crash_date) AS crash_year
    FROM {{ source('staging','person_data') }}
    WHERE  age != 0
    
    
)




select *,
cast(crash_date as timestamp) as traffic_crash_date,
{{ convert_month('crash_month_number') }} AS crash_month

from 
persons_data

-- dbt build --select <model.sql> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}




