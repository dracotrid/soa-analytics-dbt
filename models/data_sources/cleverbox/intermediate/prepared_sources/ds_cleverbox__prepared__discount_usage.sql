WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__discount_usage') }}
)

{{ tf_transform_model('source') }}
