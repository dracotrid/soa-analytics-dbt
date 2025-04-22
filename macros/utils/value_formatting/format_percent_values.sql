{% macro format_percent_values(column_name) %}
    COALESCE(FORMAT("%.2f%%", {{ column_name }} * 100.0), "Not Available")
{% endmacro %}
