WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__certificates_sales') }}
)

{{ tf_transform_model('source') }}
