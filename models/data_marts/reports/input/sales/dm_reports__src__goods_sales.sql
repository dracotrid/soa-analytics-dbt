WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__goods_sales') }}
)

{{ tf_transform_model('source') }}
