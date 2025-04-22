{%- macro mtf_transform_model_fn__deduplicate(tf_source, tf_config) -%}
{#
    "Deduplicates source dataset
#}
{%- set _macro_ = "mtf_transform_model_fn__deduplicate" -%}
{{- _mtf_log(tf_source, _macro_, "tf_source", debug) -}}
{{- _mtf_log(tf_config, _macro_, "tf_config", debug) -}}
{#- "FIXME: Find out a better way to identify if there is any CTE already defined in the model" #}
{% if "with" in model.raw_code.lower() or tf_source.startswith("__mtf_internal_") -%}, {% else %}WITH {% endif -%}
__mtf_internal_deduplicated_ AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ tf_config.partition_by|join(",") }}
                {%- if tf_config.order_by %}
                ORDER BY {{ tf_config.order_by|join(",") }}
                {%- endif %}
            ) AS row_num
        {# "TODO: Think about -- tf_source is CTE and tf_ref WORKS with else!" #}
        FROM {{ tf_source if tf_source else tf_ref(tf_config.source) }}
    ) AS ranked
    WHERE row_num = 1
)
{{ tf_transform_model('__mtf_internal_deduplicated_', mtf_resolve_transform_model_conf(model, "map")) }}
{%- endmacro -%}
