{% macro _mtf_create_default_conf(tf_target, tf_mode) %}
{#
    "Creates default model transformation configuration
    "Arg:
    "   - tf_target: str -- Target model name
    "   - tf_mode: str   -- Model transformation mode
    "Returns:
    "    dict - default transformation configuration
#}
    {% set tf_config = {"target": tf_target, "op": tf_mode} %}
    {{ return(tf_config) }}
{% endmacro %}
