--to make boolean values true or false standardized and clean any spaces in the column
{% macro to_boolean_clean(column_name) %}
    case
        when lower(trim({{ column_name }})) in ('true', 't', 'yes', 'y', '1') then true
        when lower(trim({{ column_name }})) in ('false', 'f', 'no', 'n', '0') then false
        else null
    end
{% endmacro %}