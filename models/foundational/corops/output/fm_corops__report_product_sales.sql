WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__src__product_sales') }}
)

{{ tf_transform_model('source') }}
