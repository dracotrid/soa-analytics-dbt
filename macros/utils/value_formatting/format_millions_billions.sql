{% macro format_millions_billions(column_name, replace_zeros=False) %}
CASE
        WHEN ( {{ column_name }} = 0 AND {{replace_zeros}} ) THEN "None"
        WHEN (
            ({{ column_name }} IS NOT NULL)
            AND (ABS({{ column_name }}) < 1000000000)
        ) THEN FORMAT("$%.2fM", ROUND(({{ column_name }} / 1000000), 2))
        WHEN (
            ({{ column_name }} IS NOT NULL)
            AND (ABS({{ column_name }}) > 999999999)
        ) THEN FORMAT("$%.2fB", ROUND(({{ column_name }} / 1000000000), 2))
        ELSE "Not Available"
    END
{% endmacro %}
