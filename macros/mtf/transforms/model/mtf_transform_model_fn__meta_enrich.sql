{%- macro mtf_transform_model_fn__meta_enrich(tf_source, tf_config) -%}
{#
    "Parse a field and uses values for other fields
#}
{%- set _macro_ = "mtf_transform_model_fn__meta_enrich" -%}
{{- _mtf_log(tf_source, _macro_, "tf_source", debug) -}}
{{- _mtf_log(tf_config, _macro_, "tf_config", debug) -}}
{% if "with" in model.raw_code.lower() or (tf_source.startswith is defined and tf_source.startswith("__mtf_internal_")) -%}, {% else %}WITH {% endif -%}
__mtf_internal_meta_enriched__raw_meta_data AS (
  SELECT
    SPLIT(LEFT(_FILE_NAME, STRPOS(_FILE_NAME, '.') - 1), '___') AS __mtf_internal_meta_raw_meta_data,
    *
  FROM {{ tf_source }}
)
,__mtf_internal_meta_enriched__meta_data AS (
  SELECT
    CASE
      WHEN ARRAY_LENGTH(__mtf_internal_meta_raw_meta_data)<1 THEN ["UNKNOWN", "sv1", ""]
      WHEN ARRAY_LENGTH(__mtf_internal_meta_raw_meta_data)=1 THEN [__mtf_internal_meta_raw_meta_data[0], "sv1",  ""]
      WHEN ARRAY_LENGTH(__mtf_internal_meta_raw_meta_data)=2 THEN [__mtf_internal_meta_raw_meta_data[0], __mtf_internal_meta_raw_meta_data[1], ""]
      WHEN ARRAY_LENGTH(__mtf_internal_meta_raw_meta_data)>2 THEN [__mtf_internal_meta_raw_meta_data[0], __mtf_internal_meta_raw_meta_data[1], __mtf_internal_meta_raw_meta_data[2]]
    END AS __mtf_internal_meta__meta_data,
    * EXCEPT(__mtf_internal_meta_raw_meta_data)
  FROM __mtf_internal_meta_enriched__raw_meta_data
)
,__mtf_internal_meta_enriched__parsed AS (
  SELECT
    __mtf_internal_meta__meta_data[0] AS __mtf_internal_meta_file_name,
    REGEXP_EXTRACT(__mtf_internal_meta__meta_data[1], r'^sv(\d+)$') AS __mtf_internal_meta_schema_version,
    (
      SELECT PARSE_JSON('{' || STRING_AGG(pair) || '}')
      FROM (
        SELECT
          1 AS _id,
          IF(ARRAY_LENGTH(SPLIT(prop, "|")) < 2, "", FORMAT('"%s":"%s"', SPLIT(prop, "|")[0], SPLIT(prop, "|")[1]))  AS pair
        FROM
          UNNEST(SPLIT(__mtf_internal_meta__meta_data[2], "__")) AS prop
      )
      WHERE pair != ""
      GROUP BY _id
    ) AS __mtf_internal_meta_json_fields,
    *
  FROM __mtf_internal_meta_enriched__meta_data
)
,__mtf_internal_meta_enriched AS (
    SELECT * EXCEPT(__mtf_internal_meta__meta_data, __mtf_internal_meta_file_name)
    FROM __mtf_internal_meta_enriched__parsed
)

{{ tf_transform_model('__mtf_internal_meta_enriched', mtf_resolve_transform_model_conf(model, "map")) }}
{%- endmacro -%}
