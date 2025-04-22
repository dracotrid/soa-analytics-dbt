{% macro detect_historical_changes(
    relation,
    id_column,
    loaded_at_column,
    compare_columns
) %}

{% set filtered_compare_columns = compare_columns | reject('in', [id_column, loaded_at_column]) | list %}

{% set created_case %}
    {% for column in filtered_compare_columns %}
        old_{{ column }} IS NULL
        {% if not loop.last %}AND{% endif %}
    {% endfor %}

    AND (
        {% for column in filtered_compare_columns %}
            {{ column }} IS NOT NULL
            {% if not loop.last %}OR{% endif %}
        {% endfor %}
    )
{% endset %}

{% set updated_case %}
    {% for column in filtered_compare_columns %}
        {{ column }} IS DISTINCT FROM old_{{ column }}
        {% if not loop.last %}OR{% endif %}
    {% endfor %}
{% endset %}

WITH source_data AS (
    SELECT * FROM {{ relation }}
),

lagged_data AS (
    SELECT
        {{ id_column }},
        {{ loaded_at_column }},

        {% for column in filtered_compare_columns %}
            {{ column }},
            LAG({{ column }}) OVER (PARTITION BY {{ id_column }} ORDER BY {{ loaded_at_column }}) AS old_{{ column }}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM source_data
),

maybe_changes AS (
    SELECT
        *,
        CASE
        -- Created
            WHEN ({{ created_case }})
                THEN {{ loaded_at_column }}
            -- Updated
            WHEN ({{ updated_case }})
                THEN {{ loaded_at_column }}
        END AS change_date,
        CASE
        -- Created
            WHEN ({{ created_case }})
                THEN "Created"
        -- Updated
            WHEN ({{ updated_case }})
                THEN "Updated"
        END AS change_type,
        ARRAY_CONCAT(
            {% for column in filtered_compare_columns %}
                CASE WHEN {{ column }} IS DISTINCT FROM old_{{ column }} THEN ["{{ column }}"] ELSE [] END
                {% if not loop.last %},{% endif %}
            {% endfor %}
        ) AS changed_columns
    FROM lagged_data
),

changes AS (
    SELECT
        * EXCEPT ({{ loaded_at_column }}, changed_columns),
        CASE
            WHEN ARRAY_LENGTH(changed_columns) > 0
                THEN ARRAY_TO_STRING(changed_columns, ";")
        END AS changed_columns
    FROM maybe_changes
    WHERE change_type IS NOT NULL
)

SELECT *
FROM changes
ORDER BY change_date DESC, {{ id_column }}

{% endmacro %}
