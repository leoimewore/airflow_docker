{% macro update_pedestrain_info(make) -%}

    case
        when {{make}} is null then 'PEDESTRAIN'
        else {{make}}
    end

{%- endmacro %}