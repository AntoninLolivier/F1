{% macro first_name_last_name_to_full_name(first_name_column_name, last_name_column_name) %}
    {{ first_name_column_name }} || ' ' || {{ last_name_column_name }}
{% endmacro %}