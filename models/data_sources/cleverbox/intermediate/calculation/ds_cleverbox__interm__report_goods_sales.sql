WITH employees_speciality AS (
    SELECT
        name AS employees_speciality_name,
        job_title AS speciality
    FROM {{ tf_ref('ds_cleverbox__processed__employees') }}
    GROUP BY name, job_title
),

bonus_report_goods AS (
    SELECT
        id AS bonus_id,
        bonus_unit,
        bonus_total,
        cleverbox_bonus_total
    FROM {{ tf_ref('ds_cleverbox__interm__bonus_report_goods_sales') }}
),

certificates_balance AS (
    SELECT
        yuid AS certificates_balance_id,
        sum(sum) AS certificates_balance_sum
    FROM {{ tf_source('ds_cleverbox__raw__certificates_and_balance_goods') }}
    GROUP BY yuid
),

report_goods_step_1 AS (
    SELECT
        *,
        coalesce(cost, 0) * coalesce(amount, 0) AS cost_total,
        coalesce(cost_price_unit, 0) * coalesce(amount, 0) AS calculated_cost_price_total,
        coalesce(cost, 0) - coalesce(price, 0) AS discount_unit,
        coalesce(price, 0) * coalesce(amount, 0) AS full_income_total,
        coalesce(price, 0) - coalesce(cost_price_unit, 0) - coalesce(bonus_unit, 0) AS profit_unit
    FROM {{ tf_ref('ds_cleverbox__processed__goods_sales') }} AS goods_sales
    LEFT JOIN employees_speciality
        ON goods_sales.employee = employees_speciality.employees_speciality_name
    LEFT JOIN bonus_report_goods
        ON goods_sales.id = bonus_report_goods.bonus_id
    LEFT JOIN certificates_balance
        ON goods_sales.id = certificates_balance.certificates_balance_id
),

report_goods_step_2 AS (
    SELECT
        *,
        discount_unit * coalesce(amount, 0) AS discount_total,
        full_income_total - coalesce(certificates_balance_sum, 0) AS income_total,
        round(CASE WHEN profit_unit = 0 THEN 0 ELSE coalesce(price, 0) / profit_unit END, 2) AS payback
    FROM report_goods_step_1
),

report_goods_step_3 AS (
    SELECT
        *,
        coalesce(income_total, 0) - coalesce(calculated_cost_price_total, 0) - coalesce(bonus_total, 0) AS profit_total
    FROM report_goods_step_2
),

final AS (
    SELECT
        *,
        CASE WHEN full_income_total = 0 THEN 0 ELSE profit_total / full_income_total END AS margin
    FROM report_goods_step_3
)

{{ tf_transform_model('final') }}
