{#
    "Contains Field Value Extractors macros:
    "   - tf__fve__identity
    "   - tf__fve__value_or_default
    "   - tf__fve__array_field_value
#}

{% macro tf__fve__identity(value) -%}
{#
    "Returns provided value back
#}
    {{ return(value) }}
{%- endmacro %}


{% macro tf__fve__value_or_default(field_expression, default_value="FALSE") -%}
{#
    "Returns field value if it is set or provided default value
#}
  {{ return("IFNULL(" ~ field_expression ~ ", " ~ default_value ~ ")") }}
{%- endmacro %}


{% macro tf__fve__array_field_value(array_field, array_alias, value_path, priority, target_type) -%}
{#
    "Extracts value of the most relevant entry from a json array field according to priority
    "
    "Parameters:
    "   - array_field: str -- json array field name
    "   - array_alias: str -- json array field alias
    "   - value_path: str  -- relative json path to embedded value (path.to.value)
    "   - priority: list   -- defines a way to identify the most relevant entry, default is natural order
    "         value_path: str -- relative json path to sort value (i.e. path.to.sort.value)
    "         expression: str -- expression to be used to sort the values, default is '[value]'
    "         type: str       -- data type to cast the value to
    "         sort: str       -- ASC or DESC, default is ASC
    "         safe_cast: bool -- whether to use SAFE_CAST or CAST over the value, default is True
    "   - target_type: str -- defines a type to which output should be able to be casted (CAST 'output' AS 'target_type')
#}
{%- set priority = priority if priority is defined else [] -%}
{%- set result_field = array_alias ~ "." ~ value_path -%}
SELECT {% if target_type == "json" -%}{{ result_field }}{%- else -%}JSON_VALUE({{ result_field }}){%- endif %}
FROM UNNEST(JSON_EXTRACT_ARRAY({{ array_field }})) AS {{ array_alias }}
{%- for item in priority %}
    {{ "ORDER BY" if loop.first else "" }}
    {%- set expression = item.expression if item.expression is defined else "[value]" %}
    {%- set safe_cast = item.safe_cast if item.safe_cast is defined else true %}
    {%- set value = ("SAFE_CAST" if safe_cast else "CAST") ~ "(JSON_VALUE(" ~ array_alias ~ "." ~ item.value_path ~ ") AS " ~ item.type ~ ")" %}
    {%- set target_order_expression = " (" ~ modules.re.sub("\[value\]", value, expression) ~ ")" -%}
    {{ target_order_expression }} {{ item.sort }}
    {{- "" if loop.last else "," -}}
{%- endfor %}
LIMIT 1
{%- endmacro %}
