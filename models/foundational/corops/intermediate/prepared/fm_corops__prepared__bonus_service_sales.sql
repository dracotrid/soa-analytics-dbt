WITH service_sales AS (
    SELECT
        eid AS service_sales__eid,
        profit_total,
        profit_total_test
    FROM {{ tf_ref('fm_corops__processed__service_sales') }}
),

source AS (
    SELECT *
    FROM {{ tf_ref('fm_corops__processed__bonus_service_sales') }}
    LEFT JOIN service_sales
        ON eid = service_sales__eid
)

{{ tf_transform_model('source') }}
