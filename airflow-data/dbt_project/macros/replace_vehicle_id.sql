{% macro replace_vehicle_id(vehicle_id) -%}
    CASE 
        WHEN NULLIF({{ vehicle_id }}, '') IS NULL THEN 1
        ELSE CAST({{ vehicle_id }} AS INT64)
    END

{%- endmacro %}