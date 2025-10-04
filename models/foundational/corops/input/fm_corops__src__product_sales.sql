WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__product_sales') }}
)

{{ tf_transform_model('source') }}
