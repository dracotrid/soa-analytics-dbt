WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__service_sales_2024') }}
    UNION ALL
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__service_sales_v1') }}
)

{{ tf_transform_model('source') }}
