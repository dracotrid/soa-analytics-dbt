{% macro _mtf_resolve_transform_model_conf_fn(tf_config) %}
{#
    "Loads model configuration loader depending on target transformation operation
    "Arg:
    "   - tf_config: dict -- DBT model transformation configuration
    "Requires: tf_config.op
    "Returns:
    "    callable - transformation configuration loader
#}
    {% set _macro_ = "_mtf_resolve_transform_model_conf_fn" %}
    {{ _mtf_log(tf_config, _macro_, "tf_config", debug) }}

    {% set tf_configuration_loader = _mtf_load_macro(
        "mtf_load_transform_conf__" ~ tf_config.op,
        "configuration loader macro not found"
       )
    %}
    {{ return(tf_configuration_loader) }}
{% endmacro %}
