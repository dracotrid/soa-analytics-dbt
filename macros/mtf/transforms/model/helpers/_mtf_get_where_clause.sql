{% macro _mtf_get_where_clause(tf_config) -%}
{#
    "Assembles 'WHERE' clause expression
#}
    {% set tf_filter = [] %}
    {% if (tf_config.filter) %}
        {% set tf_filter_config = tf_config.filter %}
        {%- if tf_filter_config is string %}
            {% do tf_filter.append(tf_filter_config) %}
        {%- elif tf_filter_config.expression %}
            {% do tf_filter.append(tf_filter_config.expression) %}
        {%- else %}
            {{ exceptions.raise_compiler_error("Unable to assemble WHERE clause for filter config: " ~ tf_filter_config) }}
        {%- endif %}
    {%- endif %}
    {% if (tf_config.parse_version) %}
        {% do  tf_filter.append("__mtf_parser_version = '" ~ tf_config.parse_version ~ "'") %}
    {%- endif %}
    {{ tf_filter | join(' AND ') }}
{%- endmacro %}
