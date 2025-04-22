{% macro mtf_load_transform_conf__merge(dbt_model, tf_config, debug=false) %}
{#
    "Assembles merge (many->one) Model Transformation Configuration
    "Args:
    "   dbt_model: dict -- DBT Graph Node
    "   tf_config: dict -- Partial (base) Model Transformation Configuration
    "Returns:
    "   dict -- Merge Model Transformation Configuration
#}
    {%- set _macro_ = "mtf_load_transform_conf__merge" -%}
    {{ _mtf_log(dbt_model, _macro_, "dbt_model", debug) }}
    {{ _mtf_log(tf_config, _macro_, "tf_config", debug) }}

    {% set dbtm_tf_config = dbt_model.config.meta.tf_config if dbt_model.config.meta.tf_config else {} %}
    {{ _mtf_log(dbtm_tf_config, _macro_, "dbtm_tf_config", debug) }}

    {# "Identify Sources" #}
    {% set _sources = {} %}
    {% if dbtm_tf_config.mergers and not dbtm_tf_config.base_relation %}
        {{ _mtf_exception("[base_relation] is required when [mergers] are defined in the MTF configuration", _macro_) }}
    {% endif %}
    {% set base_ref_name = dbtm_tf_config.base_relation if dbtm_tf_config.base_relation else dbt_model.refs[0].name %}
    {% set _source = {"alias": base_ref_name.replace(".", "__"), "name": base_ref_name } %}
    {{ tf_config.update({"primary_source": {"alias": _source.alias, "name": _source.name}}) }}
    {{ _sources.update({_source.name: _source.alias}) }}

    {% set default_merge_condition = __mtf_make_condition__merge(dbtm_tf_config.merge_condition) %}
    {% set default_merge_type = dbtm_tf_config.merge_type.upper() if dbtm_tf_config.merge_type else "LEFT" %}
    {{ tf_config.update({"secondary_sources": [] }) }}
    {% if dbtm_tf_config.mergers %}
        {% set tf_mergers = dbtm_tf_config.mergers %}
    {% else %}
        {% set tf_mergers = [] %}
        {% for dbt_ref in dbt_model.refs[1:] %}
            {{ tf_mergers.append({"relation": dbt_ref.name}) }}
        {% endfor %}
    {% endif %}
    {{ _mtf_log(tf_mergers, _macro_, "tf_mergers", debug) }}

    {% if tf_mergers|length + 1 != dbt_model.refs|length %}
        {# "TODO: Figure out another way to improve misconfiguration checks" #}
        {# { _mtf_exception("Number of referenced models in SQL does not match with defined in MTF configuration", _macro_) } #}
    {% endif %}

    {% for merger in tf_mergers %}
        {% set _source_alias = merger.relation.replace(".", "__") %}

        {% set _source_condition = {
                "left": tf_config.primary_source.alias,
                "right": _source_alias,
                "matcher": "="
            }
        %}
        {% set merge_condition = __mtf_make_condition__merge(merger.condition) if merger.condition else default_merge_condition %}
        {% if not merge_condition %}
            {{ _mtf_exception("Merge condition for [" ~ merger.relation ~ "] must be set on Model or Merger level", _macro_) }}
        {% endif %}
        {% if merge_condition.split(" ")|length == 1 %}
            {{ _source_condition.update({"field": merge_condition}) }}
        {% else %}
            {{ _source_condition.update({"expression": merge_condition}) }}
        {% endif %}

        {% set _source = {
                "alias": _source_alias,
                "name": merger.relation,
                "condition": _source_condition,
                "type": merger.get("type", default_merge_type)
            }
        %}

        {% if _sources[_source.name] %}
            {{ _mtf_exception("[" ~ _source.name ~ "] is referenced more then once", _macro_) }}
        {% else %}
            {{ tf_config.secondary_sources.append(_source) }}
            {{ _sources.update({_source.name: _source.alias}) }}
        {% endif %}
    {% endfor %}

    {# "Identify columns mapping" #}
    {{ tf_config.update({"columns": []}) }}
    {% set tf_config_columns = tf_config.columns %}

    {# "Identify defaults" #}
    {% set default_merge_func = dbtm_tf_config.get("merge_func", "COALESCE") %}
    {% set default_merge_order = dbtm_tf_config.get(
            "merge_order",
            [tf_config.primary_source.name] + tf_config.secondary_sources|map(attribute="name")|list
       )
    %}
    {% set default_field_value_extractor = dbtm_tf_config.get("merge_value_extractor", "tf__fve__identity") %}

    {% set merge_col_names = [] %}
    {% for col_name in dbt_model.columns %}
        {% set dbtmc = dbt_model.columns[col_name] %}
        {% set dbtmc_tf_config = dbtmc.meta.tf_config if dbtmc.meta.tf_config else {} %}
        {{ _mtf_log(dbtmc_tf_config, _macro_, "dbtmc_tf_config", debug) }}

        {% set merge_col_name = dbtmc_tf_config.field if dbtmc_tf_config.field else dbtmc.name %}
        {% set tf_column_config = {
            "field": merge_col_name,
            "order": dbtmc_tf_config.merge_order,
            "func": dbtmc_tf_config.merge_func,
           } if dbtmc_tf_config else {
            "field": merge_col_name,
           }
        %}
        {{ _mtf_dict_set_value_if_undefined(tf_column_config, "order", default_merge_order) }}
        {{ _mtf_dict_set_value_if_undefined(tf_column_config, "func", default_merge_func) }}
        {{ _mtf_log(tf_column_config, _macro_, "tf_column_config", debug) }}

        {% set dbtmc_value_extractors = dbtmc_tf_config.get("merge_value_extractor", {}) %}
        {% if not dbtmc_value_extractors %}
            {% for s_name in _sources %}
                {{ dbtmc_value_extractors.update({s_name: default_field_value_extractor}) }}
            {% endfor %}
        {% endif %}
        {% if dbtmc_value_extractors|length != _sources|length %}
            {{ _mtf_exception(
                "Number of Value Extractor do not match to number of referenced DBT models for field: " ~ dbtmc.name,
                _macro_
               )
            }}
        {% endif %}
        {% for s_name, s_fve in dbtmc_tf_config.get("+merge_value_extractor", {}).items() %}
            {{ dbtmc_value_extractors.update({s_name: s_fve}) }}
        {% endfor %}
        {{ tf_column_config.update({"value_extractor": dbtmc_value_extractors}) }}

        {% if merge_col_name not in merge_col_names %}
            {{ tf_config.columns.append({
                "source": tf_column_config,
                "target": {"field": merge_col_name}
               })
            }}
            {{ merge_col_names.append(merge_col_name) }}
        {% endif %}
    {% endfor %}

    {# "Identify filter -- TODO: Review if it is needed to be extracted" #}
    {% if dbtm_tf_config.filter %}
        {{ tf_config.update({"filter": dbtm_tf_config.filter}) }}
    {% endif %}

    {# "TODO: Review if it is needed to be set here" #}
    {{ tf_config.update({"source_aliases": _sources}) }}

    {{ return(tf_config) }}
{% endmacro %}


{% macro __mtf_make_condition__merge(tf_condition) %}
     {# "TODO: Extend merge_condition to be a bit more complex expression" #}
    {{ return(tf_condition) }}
{% endmacro %}
