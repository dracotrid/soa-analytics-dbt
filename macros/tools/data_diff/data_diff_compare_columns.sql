{# Calculates statistics or row by row comparison of dev and prod versions of a model;  #}
{# will work only if model defines unique and non-null key; #}
{# composite keys not supported as of now #}

{% macro data_diff_compare_columns(
    dev_relation,
    prod_relation,
    primary_key,
    exclude_columns=[]
) -%}


WITH audit_cte AS (
    SELECT
        column_name,
        perfect_match,
        null_in_a AS null_in_dev,
        null_in_b AS null_in_prod,
        missing_from_a AS missing_from_dev,
        missing_from_b AS missing_from_prod,
        conflicting_values
    FROM ({{
        audit_helper.compare_all_columns(
            a_relation=dev_relation,
            b_relation=prod_relation,
            primary_key=primary_key,
            exclude_columns=exclude_columns,
            summarize=true
        )
    }})
)

SELECT *
FROM audit_cte
WHERE
    conflicting_values > 0
    OR null_in_dev != null_in_prod
    OR missing_from_dev > 0
    OR missing_from_prod > 0
{% endmacro %}
