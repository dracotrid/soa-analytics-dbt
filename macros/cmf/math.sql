{#
    "Contains Core math helpers:
    "   - cmf_divide
    "   - cmf_multiply
    "   - cmf_sum
    "   - cmf_difference
#}

{% macro cmf_divide(numerator_column_name, denominator_column_name) %}
{#
    "Check fields and divide
#}
CASE WHEN
    IFNULL({{ denominator_column_name }}, 0) = 0
    OR IFNULL({{ numerator_column_name }}, 0) = 0
THEN 0
ELSE {{ numerator_column_name }}/{{ denominator_column_name }} END
{% endmacro %}

{% macro cmf_multiply(multiplicand_column_name, multiplier_column_name) %}
{#
    "Check fields and multiply
#}
IFNULL({{ multiplicand_column_name }}, 0)*IFNULL({{ multiplier_column_name }}, 0)
{% endmacro %}
