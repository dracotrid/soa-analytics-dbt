{% macro format_thousands(column_name) %}
-- Explanation of the format: 
-- % represents the value, so just adding $ before will add the desired result
-- .2f represents the decimal value of 2 fractional places after the "."
   (FORMAT("$%'.2f", CAST({{ column_name }} as NUMERIC)))
{% endmacro %}
