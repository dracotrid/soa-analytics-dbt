{% test custom_not_null_except_list(model, column_name, exception_keys) %}

WITH filtered_data AS (
    SELECT *
    FROM {{ model }}
    WHERE
        {{ column_name }} IS NULL
        AND _surrogate_key NOT IN ({% for key in exception_keys %}"{{ key }}"{% if not loop.last %}, {% endif %}{% endfor %})
)

SELECT *
FROM filtered_data

{% endtest %}
