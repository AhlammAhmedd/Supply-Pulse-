
{% macro clean_null(column_name) %}
    case
        when trim({{ column_name }}) in ('', 'null', 'NULL', 'N/A', 'nan', 'None', '-') then null
        else trim({{ column_name }})
    end
{% endmacro %}