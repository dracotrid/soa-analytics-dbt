{% macro mtf_resolve_transform_model_fn(tf_config, debug=false) %}
{#
    "Resolves suitable model transformation macro based on tf_config.op value
#}
    {%- set _macro_ = "mtf_resolve_transform_model_fn" -%}
    {%- set _ = log(_macro_ ~ " :: tf_config: " ~ tf_config, info=true) if debug else "" -%}

    {% set fn_name = "mtf_transform_model_fn__" ~ tf_config.op %}
    {% set transform_fn = context.get(fn_name) %}
    {% if transform_fn %}
        {{ return(transform_fn) }}
    {% else %}
        {{ exceptions.raise_compiler_error("[" ~ fn_name ~ "] model transformation macro not found") }}
    {% endif %}
{% endmacro %}
