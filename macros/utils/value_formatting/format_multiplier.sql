{% macro format_multiplier(column_name) %}
    COALESCE(FORMAT("%.2fx", {{ column_name }}), "Not Available")
{% endmacro %}
