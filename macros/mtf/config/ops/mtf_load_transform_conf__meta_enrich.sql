{% macro mtf_load_transform_conf__meta_enrich(tf_model, tf_config, debug=false) %}
{#
    "Assembles parce Model Transformation Configuration
    "Args:
    "   dbt_model: dict -- DBT Graph Node
    "   tf_config: dict -- Partial (base) Model Transformation Configuration
    "Returns:
    "   dict -- Merge Model Transformation Configuration
#}
    {%- set _macro_ = "mtf_load_transform_conf__meta_enrich" -%}
    {%- set _ = log(_macro_ ~ " :: tf_model: " ~ tf_model, info=true) if debug else "" -%}
    {%- set _ = log(_macro_ ~ " :: tf_config :: " ~ tf_config, info=true) if debug else "" -%}

    {{ return(tf_config) }}
{% endmacro %}
