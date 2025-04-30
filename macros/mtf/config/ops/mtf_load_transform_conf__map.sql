{% macro mtf_load_transform_conf__map(tf_model, tf_config, debug=false) %}
{#
    "Assembles flat (model->model) transformation configuration
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
    {% if tf_model.depends_on.nodes %}
        {% set dbt_node_fqn = tf_model.depends_on.nodes[0].split(".") %}
        {{ tf_config.update({"source": dbt_node_fqn[2] ~ "." ~ dbt_node_fqn[3] }) }}
    {% elif tf_model_config.source %}
        {{ tf_config.update({"source": tf_model_config.source}) }}
    {% endif %}
    {# "Add filter" #}
    {{ tf_config.update({"filter": tf_model_config.filter}) }}

    {# "Create columns mapping based on defined columns in target model (loaded from yaml)" #}
    {# "Extend predefined BigQuery types mapping" #}
    {# "TODO: Find out the way to use DBT Contract mapping" #}
    {{ api.Column.TYPE_LABELS.update({"INT": "INT64", "STR": "STRING"})}}
    {% set custom_data_types_provider = _mtf_load_macro(
        "mtf__extension__config__custom_data_types", raise_error=false) %}
    {% if custom_data_types_provider %}
        {{ api.Column.TYPE_LABELS.update(custom_data_types_provider())}}
    {% endif %}

    {{ tf_config.update({"columns": []}) }}
    {{ tf_config.update({"contract_enforced": tf_model.get('contract', {}).get('enforced', true)}) }}
    {% set tf_config_columns = tf_config.columns %}
    {% for col_name in tf_model.columns %}
        {% set tf_model_column = tf_model.columns[col_name] %}
        {% set rel_col_type = api.Column.translate_type(tf_model_column.data_type)|upper() %}

        {# "Check if tf config is provided otherwise create a simple mapping col->col" #}
        {% if tf_model_column.meta.tf_config  %}
            {% if tf_model_column.meta.tf_config|length == 1 and "default" in tf_model_column.meta.tf_config %}
                {% set raw_tf_source_col_config =
                    {
                        "field": tf_model_column.name,
                        "default": tf_model_column.meta.tf_config.default
                    }
                %}
             {% else %}
                {% set raw_tf_source_col_config = tf_model_column.meta.tf_config %}
             {% endif %}
        {% else %}
            {% set raw_tf_source_col_config = {"field": tf_model_column.name } %}
        {% endif %}
        {% if raw_tf_source_col_config.mapped %}
            {% if raw_tf_source_col_config.mapped is string %}
                {% set tf_source_col_config = {"mapped": {"mapping": raw_tf_source_col_config.mapped}} %}
            {% else %}
                {% set tf_source_col_config = raw_tf_source_col_config %}
            {% endif %}
            {% if not tf_source_col_config.mapped.mapping %}
                {{ _mtf_exception(
                    "'mapping' attribute must be specified in 'mapped' config for [" ~ tf_model_column.name ~ "] field",
                    _macro_)
                }}
            {% endif %}
            {% if not tf_source_col_config.mapped.field %}
                {{ tf_source_col_config.mapped.update({"field": tf_model_column.name}) }}
            {% endif %}
            {% if not tf_source_col_config.mapped.current_value %}
                {{ tf_source_col_config.mapped.update({"current_value": "current_value"}) }}
            {% endif %}
            {% if not tf_source_col_config.mapped.target %}
                {{ tf_source_col_config.mapped.update({"target_value": "target_value"}) }}
            {% endif %}
        {% else %}
            {% set tf_source_col_config = raw_tf_source_col_config %}
        {% endif %}
        {{ tf_config_columns.append({
            "source": tf_source_col_config,
            "target": {"field": tf_model_column.name, "type": rel_col_type}
           })
        }}
    {% endfor %}
    {{ return(tf_config) }}
{% endmacro %}
