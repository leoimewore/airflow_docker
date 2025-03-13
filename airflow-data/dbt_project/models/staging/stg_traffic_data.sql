{{
    config(
        materialized='view'
    )
}}


with traffic_crashes_data as (
    select *
    from {{ source('staging','crash_data') }}
)


select *  
from 
traffic_crashes_data

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

       limit 100

{% endif %}