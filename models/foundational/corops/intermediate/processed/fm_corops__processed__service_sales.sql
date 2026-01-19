WITH bonus_service_sales AS (
    SELECT
        eid AS bonus_eid,
        bonus_total
    FROM {{ tf_ref('fm_corops__processed__bonus_service_sales') }}
),

processed_step_1_source AS (
    SELECT
        *,
        income_total - cost_price_total - coalesce(bonus_total, 0) AS profit_total
    FROM {{ tf_ref('fm_corops__src__service_sales') }} AS scr_service_sales
    LEFT JOIN bonus_service_sales
        ON scr_service_sales.eid = bonus_service_sales.bonus_eid
),

processed_step_2_source AS (
    SELECT
        *,
        CASE WHEN income_total = 0 THEN 0 ELSE round(bonus_total / income_total, 0) END AS bonus_margin,
        CASE WHEN income_total = 0 THEN 0 ELSE round(profit_total / income_total, 0) END AS margin,
        CASE WHEN profit_total = 0 THEN 0 ELSE round(cost_total / profit_total, 2) END AS payback
    FROM processed_step_1_source
),

final AS (
    SELECT * FROM processed_step_2_source
)

{{ tf_transform_model('final') }}
