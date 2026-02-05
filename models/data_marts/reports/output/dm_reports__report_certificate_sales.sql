WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__prepared__certificate_sales') }}
)

{{ tf_transform_model('source') }}
