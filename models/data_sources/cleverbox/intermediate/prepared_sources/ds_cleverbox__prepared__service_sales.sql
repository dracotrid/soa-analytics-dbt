WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__service_sales_2024') }}
    UNION ALL
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__service_sales_2025') }}
)

{{ tf_transform_model('source') }}
