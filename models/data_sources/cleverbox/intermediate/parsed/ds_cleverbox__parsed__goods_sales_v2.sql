WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__enriched__goods_sales') }}
)

{{ tf_transform_model('source') }}
