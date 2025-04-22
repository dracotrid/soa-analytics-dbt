{%- macro mtf_transform_model_fn__merge(tf_source, tf_config, debug=false) -%}
{#
    "Merges multiple models togather resolving each of the target columns individually
#}
{%- set _macro_ = "mtf_transform_model_fn__merge" %}
{{- _mtf_log(tf_source, _macro_, "tf_source", debug) }}
{{- _mtf_log(tf_config, _macro_, "tf_config", debug) }}
{#- "FIXME: Find out a better way to identify if there is any CTE already defined in the model" #}
{% if "with" in model.raw_code.lower() -%}, {% else %}WITH {% endif -%}
__mtf_internal_merged_ AS (
    SELECT
        {%- if tf_config.columns %}
            {%- set dbt_source_nodes = {} %}
            {%- for tf_source_name in tf_config.source_aliases %}
                {%- set _ = dbt_source_nodes.update({tf_source_name: mtf_find_model_node(tf_source_name)}) %}
            {%- endfor %}
            {%- for tf_column_config in tf_config.columns %}
                {{- _mtf_log(tf_column_config, "mtf_transform_model_fn__merge", "tf_column_config", debug) }}
                {%- set tf_source_field = tf_column_config.source.field %}
                {%- set __log_prefix = source_name ~ "." ~ tf_source_field + "." %}
                {%- set merge_func_params = [] %}
                {%- for tf_source_name in tf_column_config.source.order %}
                    {%- set tf_source_alias = tf_config.source_aliases.get(tf_source_name) %}
                    {%- set dbt_source_node = dbt_source_nodes.get(tf_source_name) %}
                    {%- set dbt_source_node_column = dbt_source_node.columns.get(tf_source_field) %}
                    {{- _mtf_log(dbt_source_node_column, _macro_, __log_prefix, debug) }}
                    {%- set fve_name = tf_column_config.source.value_extractor.get(tf_source_name) %}
                    {{- _mtf_log(fve_name, _macro_, __log_prefix ~ "fve_name", debug) }}
                    {%- set fve_macro = _mtf_load_macro(fve_name) %}
                    {{- _mtf_log(fve_macro, _macro_, __log_prefix ~ "fve_macro", debug) }}
                    {%- set field_expression = tf_source_alias ~ "." ~ tf_source_field if dbt_source_node_column else "NULL" %}
                    {{- _mtf_log(field_expression, _macro_, __log_prefix ~ "field_expression", debug) }}
                    {%- set fve_value = fve_macro(field_expression) %}
                    {{- _mtf_log(fve_value, _macro_, __log_prefix ~ "fve_value", debug) }}
                    {%- set _ = merge_func_params.append(fve_value) %}
                {%- endfor %}
                {{- _mtf_log(merge_func_params,_macro_, __log_prefix ~ "merge_func_params", debug) }}
        {{ tf_column_config.source.func }}({{ merge_func_params|join(",") }}) AS {{ tf_column_config.source.field }},
            {%- endfor %}
        {%- else %}
        *
        {%- endif %}
    FROM {{ tf_ref(tf_config.primary_source.name) }} AS {{ tf_config.primary_source.alias }}
    {%- for ss in tf_config.secondary_sources %}
    {{ ss.type|upper() }} JOIN {{ tf_ref(ss.name) }} AS {{ ss.alias }}
        {% if ss.condition.field -%}
        ON {{ __mtf_tm_merge__resolve_join_field(tf_config, ss, debug) }}
        {%- elif ss.condition.expression -%}
        ON {{ __mtf_tm_merge__resolve_join_expression(tf_config, ss, debug) }}
        {%- else -%}
            {{- _mtf_exception("Secondary source merge condition cannot be resolved: " ~ ss , _macro_) -}}
        {%- endif -%}
    {%- endfor %}
)
{{ tf_transform_model('__mtf_internal_merged_', mtf_resolve_transform_model_conf(model, "map")) }}
{%- endmacro %}


{%- macro __mtf_tm_merge__resolve_join_expression(tf_config, secondary_source, debug=true) -%}
    {% set _macro_ = "__mtf_tm_merge__resolve_join_expression" %}
    {% set condition = secondary_source.condition %}
    {% set re = modules.re %}
    {% set on_clause = {"value": condition.expression} %}

    {{ on_clause.update({
            "value": re.sub(
                "\[([a-z\d_]+)\](?!\s*\[.*\..*\])",
                condition.left ~ ".\\1 " ~ condition.matcher ~ " " ~ condition.right ~ ".\\1",
                on_clause.value
            )
        })
    }}
    {{ _mtf_log(on_clause.value, _macro_, secondary_source.name ~ "|on_clause-0", debug) }}

    {{ on_clause.update({
            "value": re.sub(
                "\[relation\.([a-z\d_]+)\]",
                secondary_source.alias ~ ".\\1",
                on_clause.value
            )
        })
    }}
    {{ _mtf_log(on_clause.value, _macro_, secondary_source.name ~ "|on_clause-1", debug) }}

    {{ on_clause.update({
            "value": re.sub(
                "\[base\.([a-z\d_]+)\]",
                tf_config.primary_source.alias ~ ".\\1",
                on_clause.value
            )
        })
    }}
    {{ _mtf_log(on_clause.value, _macro_, secondary_source.name ~ "|on_clause-final", debug) }}

    {{ return(on_clause.value) }}
{%- endmacro -%}


{%- macro __mtf_tm_merge__resolve_join_field(tf_config, secondary_source, debug=true) -%}
    {% set _macro_ = "__mtf_tm_merge__resolve_join_field" %}
    {% set condition = secondary_source.condition %}
    {% set on_clause =
        condition.left ~ "." ~ condition.field ~
        " " ~ condition.matcher ~ " " ~
        condition.right ~ "." ~ condition.field
    %}
    {{ _mtf_log(on_clause, _macro_, secondary_source.name ~ "|on_clause", debug) }}
    {{ return(on_clause) }}
{%- endmacro -%}
