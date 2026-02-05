WITH employees_position AS (
    SELECT
        name AS employees_position_expert_name,
        job_title AS expert_position
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
    GROUP BY name, job_title
),

report_goods_step_1 AS (
    SELECT
        *,
        cost_total - paid AS discount_total,
        paid AS income_total,
        round(paid / amount, 5) AS price,
        round(cost_price_total / amount, 5) AS cost_price_unit
    FROM {{ tf_ref('ds_cleverbox__processed__goods_sales') }} AS goods_sales
    LEFT JOIN employees_position
        ON goods_sales.expert_name = employees_position.employees_position_expert_name
),

final AS (
    SELECT * FROM report_goods_step_1
)

{{ tf_transform_model('final') }}
