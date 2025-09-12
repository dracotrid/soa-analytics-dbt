WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__output__goods_sales') }}
)

{{ tf_transform_model('source') }}
