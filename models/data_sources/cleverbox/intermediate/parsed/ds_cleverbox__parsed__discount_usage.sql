WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__discount_usage_2024') }}
    UNION ALL
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__discount_usage_2025') }}
)

{{ tf_transform_model('source') }}
