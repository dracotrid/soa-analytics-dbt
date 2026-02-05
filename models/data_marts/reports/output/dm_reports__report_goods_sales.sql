WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__prepared__goods_sales') }}
)

{{ tf_transform_model('source') }}
