WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__processed__service_sales') }}
)

{{ tf_transform_model('source') }}
