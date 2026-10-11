
{% macro to_number_clean(column_name) %}
    try_to_decimal(
        trim(replace(replace(upper(trim({{ column_name }})), 'LE', ''), ',', '.')),
        12, 2
    )
{% endmacro %}