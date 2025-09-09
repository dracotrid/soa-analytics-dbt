WITH employees_speciality AS (
    SELECT
        name AS employees_speciality_name,
        job_title AS speciality
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
    GROUP BY name, job_title
),

bonus_report_goods AS (
    SELECT
        eid AS bonus_id,
        bonus_total,
        cleverbox_bonus_total
    FROM {{ tf_ref('ds_cleverbox__interm__bonus_goods_sales') }}
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
        cost_total - paid AS discount_total,
        paid - coalesce(certificates_balance_sum, 0) AS income_total,
        round(paid / amount, 5) AS price,
        round(cost_price_total / amount, 5) AS cost_price_unit
    FROM {{ tf_ref('ds_cleverbox__processed__goods_sales') }} AS goods_sales
    LEFT JOIN employees_speciality
        ON goods_sales.employee = employees_speciality.employees_speciality_name
    LEFT JOIN bonus_report_goods
        ON goods_sales.eid = bonus_report_goods.bonus_id
    LEFT JOIN certificates_balance
        ON goods_sales.eid = certificates_balance.certificates_balance_id
),

report_goods_step_2 AS (
    SELECT
        *,
        income_total - cost_price_total - coalesce(bonus_total, 0) AS profit_total
    FROM report_goods_step_1
),

final AS (
    SELECT
        *,
        round(CASE WHEN profit_total = 0 THEN 0 ELSE paid / profit_total END, 2) AS payback,
        CASE WHEN paid = 0 THEN 0 ELSE profit_total / paid END AS margin
    FROM report_goods_step_2
)

{{ tf_transform_model('final') }}
