{% macro _mtf_get_where_clause(tf_filter_config) -%}
{#
    "Assembles 'WHERE' clause expression
#}
    {%- if tf_filter_config is string %}
        {{- tf_filter_config }}
    {%- elif tf_filter_config.expression %}
        {{- tf_filter_config.expression }}
    {%- else %}
        {{ exceptions.raise_compiler_error("Unable to assemble WHERE clause for filter config: " ~ tf_filter_config) }}
    {%- endif %}
{%- endmacro %}
