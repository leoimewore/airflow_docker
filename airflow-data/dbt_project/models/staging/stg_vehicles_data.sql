{{
    config(
        materialized='view'
    )
}}


with vehicles_data as (
    select
    * except(make,model),
    
    {{update_pedestrain_info('make')}} as make,
    {{update_pedestrain_info('model')}} as model
    from {{ source('staging','vehicle_data') }}
)


select * 
from 
vehicles_data

