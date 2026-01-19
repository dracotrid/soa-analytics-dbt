WITH bonus_goods_sales AS (
    SELECT
        eid AS bonus_goods_sales__eid,
        bonus_total
    FROM {{ tf_ref('fm_corops__processed__bonus_goods_sales') }}
),

goods_sales_step_1 AS (
    SELECT
        *,
        income_total - cost_price_total - coalesce(bonus_total, 0) AS profit_total
    FROM {{ tf_ref('fm_corops__src__goods_sales') }} AS goods_sales
    LEFT JOIN bonus_goods_sales
        ON goods_sales.eid = bonus_goods_sales.bonus_goods_sales__eid
),

goods_sales_step_2 AS (
    SELECT
        *,
        round(CASE WHEN profit_total = 0 THEN 0 ELSE paid / profit_total END, 2) AS payback,
        CASE WHEN paid = 0 THEN 0 ELSE profit_total / paid END AS margin
    FROM goods_sales_step_1
),

final AS (
    SELECT * FROM goods_sales_step_2
)

{{ tf_transform_model('final') }}
