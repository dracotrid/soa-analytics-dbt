WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__src__subscription_sales') }}
)

{{ tf_transform_model('source') }}
