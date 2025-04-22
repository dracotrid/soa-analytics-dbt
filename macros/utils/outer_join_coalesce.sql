{% macro outer_join_coalesce(table_1, table_2, key_field, value_field) %}
    {# 
        Makes a coalesce between values depending on the key field.
    #}
    CASE
        WHEN {{ table_1 }}.{{ key_field }} IS NOT NULL
            THEN {{ table_1 }}.{{ value_field }}
        ELSE {{ table_2 }}.{{ value_field }}
    END AS {{ value_field }}
{% endmacro %}
