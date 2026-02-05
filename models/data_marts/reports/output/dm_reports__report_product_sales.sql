WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__prepared__product_sales') }}
)

{{ tf_transform_model('source') }}
