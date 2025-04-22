{% macro mtf_resolve_transform_model_conf(tf_model, tf_op) %}
{#
    "Assembles model transformation configuration
    "Arg:
    "   - tf_model: dict -- DBT model context
    "Returns:
    "    dict - transformation configuration
#}
    {% if not tf_op and tf_model.config.meta.tf_config %}
        {% set tf_op = tf_model.config.meta.tf_config.op %}
    {% endif %}
    {% set tf_config = _mtf_create_default_conf(tf_model.name, tf_op if tf_op else "map") %}
    {{ return(_mtf_resolve_transform_model_conf_fn(tf_config)(tf_model, tf_config)) }}
{% endmacro %}
