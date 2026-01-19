WITH corops__bonus_service_sales AS (
    SELECT
        eid AS corops_eid,
        base_for_bonus
    FROM {{ tf_ref('fm_corops__bonus_service_sales') }}
),

source AS (
    SELECT *
    FROM {{ tf_ref('ds_cleverbox__bonus_service_sales') }} AS cleverbox__bonus_service_sales
    LEFT JOIN corops__bonus_service_sales
        ON cleverbox__bonus_service_sales.eid = corops__bonus_service_sales.corops_eid
)

{{ tf_transform_model('source') }}
