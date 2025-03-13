{% macro fill_safety_equipment(safety_equipment) -%}

    CASE 
        WHEN {{safety_equipment}} IS NULL THEN 'USAGE UNKNOWN'
        ELSE {{safety_equipment}}
    END

{%- endmacro %}