{{
    config(
        materialized='table'
    )
}}


with crash_id_data as (
    select *

    from {{ ref('stg_persons_data') }}
    where person_type != 'PASSENGER' and age > 15
)


select 
traffic_crash_date,
age,
sex,
safety_equipment,
airbag_deployed
from crash_id_data
