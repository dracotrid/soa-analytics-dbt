WITH certificates_balance AS (
    SELECT
        yuid AS certificates_balance_id,
        sum AS certificates_balance_sum
    FROM {{ tf_source('ds_cleverbox__raw__certificates_and_balance') }}
    GROUP BY yuid, sum
),

bonus AS (
    SELECT
        id AS bonus_id,
        bonus_total,
        is_vip,
        is_employee
    FROM {{ tf_ref('ds_cleverbox__interm__bonus_report_services') }}
),

intermediate_step_1_source AS (
    SELECT
        *,
        COALESCE(amount, 0) * COALESCE(cost, 0) AS cost_total,
        COALESCE(amount, 0) * COALESCE(discount, 0) AS discount_total,
        COALESCE(amount, 0) * COALESCE(cost_price_unit, 0) AS cost_price_total,
        CASE WHEN COALESCE(cost, 0) = 0 THEN 0 ELSE COALESCE(discount, 0) / COALESCE(cost, 0) * 100 END AS discount_persent,
        CASE
            WHEN COALESCE(subscription, 0) > 0 THEN 0
            WHEN COALESCE(paid, 0) - COALESCE(certificates_balance_sum, 0) < 1 THEN 0
            ELSE COALESCE(paid, 0) - COALESCE(certificates_balance_sum, 0)
        END AS income
    FROM {{ tf_ref('ds_cleverbox__interm__report_services') }} AS service_sales
    LEFT JOIN certificates_balance
        ON service_sales.id = certificates_balance.certificates_balance_id
    LEFT JOIN bonus
        ON service_sales.id = bonus.bonus_id

),

intermediate_step_2_source AS (
    SELECT
        *,
        ROUND(COALESCE(amount, 0) * COALESCE(income, 0), 2) AS income_total
    FROM intermediate_step_1_source
),

intermediate_step_3_source AS (
    SELECT
        *,
        income_total - cost_price_total - bonus_total AS profit_total
    FROM intermediate_step_2_source
),

intermediate_step_4_source AS (
    SELECT
        *,
        CASE WHEN COALESCE(income_total, 0) = 0 THEN 0 ELSE ROUND(bonus_total / income_total * 100, 0) END AS bonus_margin,
        CASE WHEN COALESCE(income_total, 0) = 0 THEN 0 ELSE ROUND(profit_total / income_total * 100, 0) END AS margin
    FROM intermediate_step_3_source
),

final AS (
    SELECT * FROM intermediate_step_4_source
)

{{ tf_transform_model('final') }}
