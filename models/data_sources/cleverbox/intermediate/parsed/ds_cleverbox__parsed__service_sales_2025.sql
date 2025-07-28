WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__service_sales_2025_q1') }}
    UNION ALL
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__service_sales_2025_q2') }}
    UNION ALL
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__service_sales_2025_q3') }}
    UNION ALL
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__service_sales_2025_q4') }}
)

{{ tf_transform_model('source') }}
