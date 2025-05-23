WITH employees_speciality AS (
    SELECT
        name AS employees_speciality_name,
        job_title AS speciality
    FROM {{ tf_ref('ds_cleverbox__prepared__employees') }}
    GROUP BY name, job_title
),

bonus_report_goods AS (
    SELECT
        id AS bonus_id,
        bonus_total
    FROM {{ tf_ref('ds_cleverbox__interm__bonus_report_goods') }}
),

report_goods_step_1 AS (
    SELECT
        *,
        COALESCE(cost, 0) * COALESCE(amount, 0) AS cost_total,
        COALESCE(cost_price_unit, 0) * COALESCE(amount, 0) AS cost_price_total,
        COALESCE(cost, 0) - COALESCE(price, 0) AS discount_unit

    FROM {{ tf_ref('ds_cleverbox__prepared__goods_sales') }} AS goods_sales
    LEFT JOIN employees_speciality
        ON goods_sales.employee = employees_speciality.employees_speciality_name
    LEFT JOIN bonus_report_goods
        ON goods_sales.id = bonus_report_goods.bonus_id
),

final AS (
    SELECT
        *,
        discount_unit * COALESCE(amount, 0) AS discount_total
    FROM report_goods_step_1
)



{{ tf_transform_model('final') }}
