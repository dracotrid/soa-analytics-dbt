WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__prepared__goods_sales') }}
)

{{ tf_transform_model('source') }}
