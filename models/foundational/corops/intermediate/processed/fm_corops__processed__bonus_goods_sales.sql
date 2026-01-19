WITH goods_sales AS (
    SELECT
        eid AS goods_sales__eid,
        paid,
        cost_total AS cost
    FROM {{ tf_ref('fm_corops__src__goods_sales') }}
),

processed_bonus_goods_sales_step_1 AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = '%ВідОплати' THEN paid
            WHEN bonus_type_for_calculation = '%ВідВартості' THEN cost
            WHEN bonus_type_for_calculation = 'Фіксована' THEN fixed_bonus_sum
            ELSE 0
        END AS base_for_bonus
    FROM {{ tf_ref('fm_corops__src__bonus_goods_sales') }} AS bonus_goods_sales
    LEFT JOIN goods_sales
        ON bonus_goods_sales.eid = goods_sales.goods_sales__eid
),

processed_bonus_goods_sales_step_2 AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN fixed_bonus_sum
            ELSE base_for_bonus * bonus_percent
        END AS bonus_total
    FROM processed_bonus_goods_sales_step_1
),

final AS (
    SELECT * FROM processed_bonus_goods_sales_step_2
)

{{ tf_transform_model('final') }}
