WITH source AS (
    SELECT * FROM {{ tf_source('ds_meta__raw__ads_metrics_2024') }}
    UNION ALL
    SELECT * FROM {{ tf_source('ds_meta__raw__ads_metrics_2025') }}
)

{{ tf_transform_model('source') }}
