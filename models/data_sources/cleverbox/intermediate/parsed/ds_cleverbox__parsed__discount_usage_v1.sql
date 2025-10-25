WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__enriched__discount_usage') }}
)

{{ tf_transform_model('source') }}
