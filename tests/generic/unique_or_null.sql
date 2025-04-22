{% test unique_or_null(model, column_name, prod_only=false) %}
    {% if prod_only and target.name == 'sandbox' %}
        SELECT NULL AS skip_test
        FROM {{ model }}
        WHERE FALSE
    {% else %}
        WITH validation AS (
            SELECT
                {{ column_name }} AS test_field,
                COUNT(*) AS row_count
            FROM {{ model }}
            GROUP BY {{ column_name }}
        )
        SELECT *
        FROM validation
        WHERE row_count > 1 AND test_field IS NOT NULL
    {% endif %}
{% endtest %}
