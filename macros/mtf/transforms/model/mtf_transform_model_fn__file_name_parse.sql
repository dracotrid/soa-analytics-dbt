{%- macro mtf_transform_model_fn__file_name_parse(tf_source, tf_config) -%}
{#
    "Parse a field and uses values for other fields
#}
{%- set _macro_ = "mtf_transform_model_fn__parse" -%}
{{- _mtf_log(tf_source, _macro_, "tf_source", debug) -}}
{{- _mtf_log(tf_config, _macro_, "tf_config", debug) -}}
{% if "with" in model.raw_code.lower() or tf_source.startswith("__mtf_internal_") -%}, {% else %}WITH {% endif -%}
__mrf_parser__raw_meta_data AS (
  SELECT
    SPLIT(LEFT({{ tf_config.parse_source_field }}, STRPOS({{ tf_config.parse_source_field }}, '.') - 1), '___') AS __mtf_parser_raw_meta_data,
    *
  FROM {{ tf_source }}
)
,__mrf_parser__meta_data AS (
  SELECT
    CASE
      WHEN ARRAY_LENGTH(__mtf_parser_raw_meta_data)<1 THEN ["UNKNOWN", "pv1", ""]
      WHEN ARRAY_LENGTH(__mtf_parser_raw_meta_data)=1 THEN [__mtf_parser_raw_meta_data[0], "pv1",  ""]
      WHEN ARRAY_LENGTH(__mtf_parser_raw_meta_data)=2 THEN [__mtf_parser_raw_meta_data[0], __mtf_parser_raw_meta_data[1], ""]
      WHEN ARRAY_LENGTH(__mtf_parser_raw_meta_data)>2 THEN [__mtf_parser_raw_meta_data[0], __mtf_parser_raw_meta_data[1], __mtf_parser_raw_meta_data[2]]
    END AS __mtf_parser__meta_data,
    * EXCEPT(__mtf_parser_raw_meta_data)
  FROM __mrf_parser__raw_meta_data
)
,__mrf_parser__parsed AS (
  SELECT
    __mtf_parser__meta_data[0] AS __mtf_parser_file_name,
    REGEXP_EXTRACT(__mtf_parser__meta_data[1], r'^pv(\d+)$') AS __mtf_parser_verion,
    (
      SELECT PARSE_JSON('{' || STRING_AGG(pair) || '}')
      FROM (
        SELECT
          1 AS _id,
          IF(ARRAY_LENGTH(SPLIT(prop, "|")) < 2, "", FORMAT('"%s":"%s"', SPLIT(prop, "|")[0], SPLIT(prop, "|")[1]))  AS pair
        FROM
          UNNEST(SPLIT(__mtf_parser__meta_data[2], "__")) AS prop
      )
      WHERE pair != ""
      GROUP BY _id
    ) AS __mtf_internal_meta_json_fields,
    *
  FROM __mrf_parser__meta_data
)
,__mrf_parser__filtered AS (
    SELECT * EXCEPT(__mtf_parser__meta_data, __mtf_parser_file_name, __mtf_parser_verion)
    FROM __mrf_parser__parsed
    WHERE __mrf_parser__parsed.__mtf_parser_verion = '{{ tf_config.parse_version }}'
)

{{ tf_transform_model('__mrf_parser__filtered', mtf_resolve_transform_model_conf(model, "map")) }}
{%- endmacro -%}
