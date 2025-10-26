{%- macro mtf_transform_model_fn__map(tf_source, tf_config, debug=false) -%}
{#
    "Resolves suitable column transformation macro based on tf_column_config.source configuration
#}
{%- set _macro_ = "mtf_transform_model_fn__map" -%}
{{- _mtf_log(tf_source, _macro_, "tf_source", debug) -}}
{{- _mtf_log(tf_config, _macro_, "tf_config", debug) -}}
{#- "FIXME: Find out a better way to identify if there is any CTE already defined in the model" #}
{% if "with" in model.raw_code.lower() or (tf_source.startswith is defined and tf_source.startswith("__mtf_internal_")) -%}, {% else %}WITH {% endif -%}
__mtf_internal_mapped_ AS (
    SELECT t.*, TO_JSON_STRING(t) AS __mtf_internal_json_fields
    FROM {{ tf_source }} AS t
)

SELECT * EXCEPT (__mtf_internal_json_fields) FROM (
    SELECT
        {%- if tf_config.columns %}
            {%- for tf_column_config in tf_config.columns %}
                {{ mtf_resolve_transform_column_fn(tf_column_config)(tf_column_config) -}},
            {%- endfor %}
            {%- if tf_config.contract_enforced == false %}
                * EXCEPT (
                    {%- for tf_column_config in tf_config.columns %}
                    {%- if tf_column_config.source.field %}
                        {{ adapter.quote(tf_column_config.source.field) }}{% if not loop.last %},{% endif %}
                    {%- endif %}
                    {%- endfor %}
                )
            {%- else %}
                __mtf_internal_json_fields
            {%- endif %}
        {%- else %}
            *
        {%- endif %}
    FROM __mtf_internal_mapped_
    {%- if tf_config.filter %}
    WHERE
        {{ _mtf_get_where_clause(tf_config.filter) }}
    {%- endif %}
)
{%- endmacro -%}
