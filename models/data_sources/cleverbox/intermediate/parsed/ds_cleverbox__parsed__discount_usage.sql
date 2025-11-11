WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__discount_usage_v1') }}
)

{{ tf_transform_model('source') }}
