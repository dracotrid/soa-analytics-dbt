{#
    Contains value extract helpers from CleverBox raw data:
        - cleverbox__extract__sale_name
#}


{% macro cleverbox__extract__sale_name(sale_name) %}
{#
    "Example of a macro definition
#}
CASE {{ sale_name }}
  WHEN "AAAA" THEN "My AAA"
  WHEN "BBB" THEN "Your BB"
  ELSE {{ sale_name }}
END
{% endmacro %}
