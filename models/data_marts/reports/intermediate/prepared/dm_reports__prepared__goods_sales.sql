WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__processed__goods_sales') }}
)

{{ tf_transform_model('source') }}
