{% test table_not_empty(model) %}
WITH validation as (
    SELECT count(*) AS row_count
    FROM {{ model }}
)
SELECT row_count
FROM validation
WHERE row_count = 0
{% endtest %}
