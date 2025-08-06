WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__goods_sales_2025') }}
)

{{ tf_transform_model('source') }}
