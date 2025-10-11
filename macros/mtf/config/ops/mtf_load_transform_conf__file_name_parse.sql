{% macro mtf_load_transform_conf__file_name_parse(tf_model, tf_config, debug=false) %}
{#
    "Assembles parce Model Transformation Configuration
    "Args:
    "   dbt_model: dict -- DBT Graph Node
    "   tf_config: dict -- Partial (base) Model Transformation Configuration
    "Returns:
    "   dict -- Merge Model Transformation Configuration
#}
    {%- set _macro_ = "mtf_load_transform_conf__map" -%}
    {%- set _ = log(_macro_ ~ " :: tf_model: " ~ tf_model, info=true) if debug else "" -%}
    {%- set _ = log(_macro_ ~ " :: tf_config: " ~ tf_config, info=true) if debug else "" -%}

    {% set tf_model_config = tf_model.config.meta.tf_config if tf_model.config.meta.tf_config else {} %}

    {%- set _ = log(_macro_ ~ " :: tf_model_config: " ~ tf_model_config, info=true) if debug else "" -%}

    {% if tf_model_config.parse_source_field %}
        {{ tf_config.update({'parse_source_field': tf_model_config.parse_source_field}) }}
    {% else %}
        {% set _ =  _mtf_exception("parse_source_field must be specified", _macro_) %}
    {% endif %}

    {% if tf_model_config.parse_version %}
        {{ tf_config.update({'parse_version': tf_model_config.parse_version}) }}
    {% else %}
        {% set _ = _mtf_exception("parse_version must be specified", _macro_) %}
    {% endif %}

    {%- set _ = log(_macro_ ~ " :: tf_config :: " ~ tf_config, info=true) if debug else "" -%}

    {{ return(tf_config) }}
{% endmacro %}
