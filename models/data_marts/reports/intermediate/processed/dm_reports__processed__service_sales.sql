WITH bonus_cleverbox_service_sales AS (
    SELECT
        eid AS bonus_cleverbox_service_sales__id,
        bonus_cleverbox_total
    FROM {{ tf_ref('dm_reports__src__bonus_service_sales') }}
),

final AS (
    SELECT *
    FROM {{ tf_ref('dm_reports__src__service_sales') }} AS service_sales
    LEFT JOIN bonus_cleverbox_service_sales
        ON service_sales.eid = bonus_cleverbox_service_sales.bonus_cleverbox_service_sales__id
)

{{ tf_transform_model('final') }}
