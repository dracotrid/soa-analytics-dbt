WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__processed__goods_sales') }}
)

{{ tf_transform_model('source') }}
