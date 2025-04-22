{#
    "Core Model Foundation Currency helper functions
#}

{% macro cmf_normalize_currency_code(currency_name) %}
{#
    "Converts provided currency name into ISO 4217 code otherwise returns back provided name
#}
CASE {{ currency_name }}
  WHEN "Euro" THEN "EUR"
  WHEN "US Dollar" THEN "USD"
  WHEN "British pound" THEN "GBP"
  ELSE {{ currency_name }}
END
{% endmacro %}
