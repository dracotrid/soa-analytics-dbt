WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__discount_usage_enriched_raw') }}
)

{{ tf_transform_model('source') }}
