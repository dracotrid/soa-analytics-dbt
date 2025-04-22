{% macro mtf_load_transform_conf__deduplicate(tf_model, tf_config) %}
{#
    "Assembles flat (model->model) transformation configuration for DEDUPLICATE operation
    "Args:
    "   tf_model: dict  -- DBT Graph Node
    "   tf_config: dict -- Partial (base) transformation configuration
    "Returns:
    "   dict -- complete transformation configuration
#}
    {%- set _macro_ = "mtf_load_transform_conf__map" -%}
    {%- set _ = log(_macro_ ~ " :: tf_model: " ~ tf_model, info=true) if debug else "" -%}
    {%- set _ = log(_macro_ ~ " :: tf_config: " ~ tf_config, info=true) if debug else "" -%}

    {% set tf_model_config = tf_model.config.meta.tf_config if tf_model.config.meta.tf_config else {} %}
    {# "Identify source node (expected the only node)" #}
    {# "TODO: Extract into a shared function as MAP also requires the only source" #}
    {% if tf_model_config.source %}
        {{ tf_config.update({"source": tf_model_config.source}) }}
    {% elif tf_model.depends_on.nodes %}
        {% set dbt_node_fqn = tf_model.depends_on.nodes[0].split(".") %}
        {{ tf_config.update({"source": dbt_node_fqn[2] ~ "." ~ dbt_node_fqn[3] }) }}
    {% endif %}

    {% if tf_model_config.deduplicate_on %}
        {% set dp = tf_model_config.deduplicate_on %}
        {# "Check if deduplicate_on is list" #}
        {% if dp is iterable and (dp is not string and dp is not mapping) %}
            {{ tf_config.update({"partition_by": tf_model_config.deduplicate_on}) }}
        {% else %}
            {{ tf_config.update({"partition_by": [tf_model_config.deduplicate_on]}) }}
        {% endif %}
    {% else %}
        {{ _mtf_exception("[deduplicate_on] must be specified", _macro_) }}
    {% endif %}

    {% if tf_model_config.deduplicate_priority %}
        {% set dp = tf_model_config.deduplicate_priority %}
        {% if dp is iterable and (dp is not string and dp is not mapping) %}
            {{ tf_config.update({"order_by": tf_model_config.deduplicate_priority}) }}
        {% else %}
            {{ tf_config.update({"order_by": [tf_model_config.deduplicate_priority]}) }}
        {% endif %}
    {% endif %}

    {{ return(tf_config) }}
{% endmacro %}
