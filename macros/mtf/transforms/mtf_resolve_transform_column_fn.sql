{% macro mtf_resolve_transform_column_fn(tf_column_config, debug=false) %}
{#
    "Resolves suitable column transformation macro based on tf_column_config.source configuration
#}
    {%- set _macro_ = "mtf_resolve_transform_column_fn" -%}
    {%- set _ = log(_macro_ ~ " :: tf_column_config: " ~ tf_column_config, info=true) if debug else "" -%}

    {% set transform_fn = {} %}
    {% set fn_name_prefix = "mtf_transform_col_fn__" %}
    {% for transform_type in tf_column_config.source %}
        {% set fn_name = fn_name_prefix ~ transform_type %}
        {% set _transform_fn = context.get(fn_name) %}
        {% if _transform_fn and not transform_fn.callable %}
            {{ transform_fn.update({"name": fn_name, "callable": _transform_fn}) }}
        {% endif -%}
    {% endfor %}
    {%- set _ = log(_macro_ ~ " :: transform_fn: " ~ transform_fn, info=true) if debug else "" -%}
    {% if transform_fn.callable %}
        {{ return(transform_fn.callable) }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unable to find column transform for " ~ tf_column_config) }}
    {% endif %}
{% endmacro %}
