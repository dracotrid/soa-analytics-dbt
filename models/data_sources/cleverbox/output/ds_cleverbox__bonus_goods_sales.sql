WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__bonus_goods_sales') }}
)

{{ tf_transform_model('source') }}
