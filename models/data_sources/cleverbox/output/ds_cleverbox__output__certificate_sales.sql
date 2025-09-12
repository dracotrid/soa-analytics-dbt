WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__certificate_sales') }}
)

{{ tf_transform_model('source') }}
