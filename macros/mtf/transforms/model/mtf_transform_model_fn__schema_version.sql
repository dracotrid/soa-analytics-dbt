{%- macro mtf_transform_model_fn__schema_version(tf_source, tf_config) -%}
{#
    "Parse a field and uses values for other fields
#}
{%- set _macro_ = "mtf_transform_model_fn__schema_version" -%}
{{- _mtf_log(tf_source, _macro_, "tf_source", debug) -}}
{{- _mtf_log(tf_config, _macro_, "tf_config", debug) -}}
{% if "with" in model.raw_code.lower() or (tf_source.startswith is defined and tf_source.startswith("__mtf_internal_")) -%}, {% else %}WITH {% endif -%}
__mtf_internal_schema_version AS (
    SELECT
        *
    FROM {{ tf_source }}
    WHERE __mtf_internal_meta_schema_version = '{{ tf_config.schema_version }}'
)
{{ tf_transform_model('__mtf_internal_schema_version', mtf_resolve_transform_model_conf(model, "map")) }}
{%- endmacro -%}
