{{
    config(
        materialized='table'
    )
}}


with crash_id_data as (
    select *,
    row_number() over(partition by person_type, crash_record_id order by traffic_crash_date desc) as rn
    from {{ ref('stg_persons_data') }}
    where person_type != 'PASSENGER' 
),

crash_info as (
    select * 
    from {{ ref('stg_traffic_data') }}
)

select
    crash_id_data.person_type,
    crash_id_data.traffic_crash_date,
    crash_id_data.crash_month,
    crash_id_data.crash_year,
    crash_id_data.safety_equipment,
    crash_info.weather_condition,
    crash_info.lighting_condition,
    crash_info.road_defect,
    crash_info.injuries_total,
    crash_info.injuries_fatal
from 
    crash_id_data
inner join crash_info
on 
crash_id_data.crash_record_id = crash_info.crash_record_id



where rn = 1