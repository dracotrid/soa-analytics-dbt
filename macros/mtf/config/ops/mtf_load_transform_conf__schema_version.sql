{% macro mtf_load_transform_conf__schema_version(tf_model, tf_config, debug=false) %}
{#
    "Assembles parce Model Transformation Configuration
    "Args:
    "   dbt_model: dict -- DBT Graph Node
    "   tf_config: dict -- Partial (base) Model Transformation Configuration
    "Returns:
    "   dict -- Merge Model Transformation Configuration
#}
    {%- set _macro_ = "mtf_load_transform_conf__schema_version" -%}
    {%- set _ = log(_macro_ ~ " :: tf_model: " ~ tf_model, info=true) if debug else "" -%}
    {%- set _ = log(_macro_ ~ " :: tf_config :: " ~ tf_config, info=true) if debug else "" -%}

    {% set tf_model_config = tf_model.config.meta.tf_config if tf_model.config.meta.tf_config else {} %}

    {% if tf_model_config.schema_version %}
        {{ tf_config.update({"schema_version": tf_model_config.schema_version}) }}
    {% else %}
        {{ _mtf_exception("[schema_version] must be specified", _macro_) }}
    {% endif %}

    {{ return(tf_config) }}
{% endmacro %}
