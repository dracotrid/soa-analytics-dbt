WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__goods_sales_v1') }}
)

{{ tf_transform_model('source') }}
