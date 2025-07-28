WITH certificates_balance AS (
    SELECT
        yuid AS certificates_balance_id,
        sum(sum) AS certificates_balance_sum
    FROM {{ tf_source('ds_cleverbox__raw__certificates_and_balance') }}
    GROUP BY yuid
),

bonus_cleverbox AS (
    SELECT
        eid AS bonus_cleverbox_id,
        bonus_cleverbox_total
    FROM {{ tf_ref('ds_cleverbox__interm__bonus_cleverbox_report_service_sales') }}
),

bonus AS (
    SELECT
        eid AS bonus_id,
        bonus_total,
        is_vip,
        is_employee
    FROM {{ tf_ref('ds_cleverbox__interm__bonus_report_service_sales') }}
),

intermediate_step_1_source AS (
    SELECT
        *,
        CASE WHEN coalesce(amount, 0) = 0 THEN 0 ELSE coalesce(paid, 0) / coalesce(amount, 0) END AS price,
        CASE WHEN coalesce(amount, 0) = 0 THEN 0 ELSE coalesce(cost_price_total, 0) / coalesce(amount, 0) END AS cost_price_unit,
        CASE WHEN coalesce(cost_total, 0) = 0 THEN 0 ELSE coalesce(discount, 0) / coalesce(cost_total, 0) * 100 END AS discount_persent,
        CASE
            WHEN coalesce(subscription, 0) > 0 THEN 0
            WHEN coalesce(paid, 0) - coalesce(certificates_balance_sum, 0) < 1 THEN 0
            ELSE coalesce(paid, 0) - coalesce(certificates_balance_sum, 0)
        END AS income_total
    FROM {{ tf_ref('ds_cleverbox__processed__report_service_sales') }} AS service_sales
    LEFT JOIN certificates_balance
        ON service_sales.eid = certificates_balance.certificates_balance_id
    LEFT JOIN bonus
        ON service_sales.eid = bonus.bonus_id

),

intermediate_step_2_source AS (
    SELECT
        *,
        coalesce(income_total, 0) - coalesce(cost_price_total, 0) - coalesce(bonus_total, 0) AS profit_total
    FROM intermediate_step_1_source
),

intermediate_step_3_source AS (
    SELECT
        *,
        CASE WHEN coalesce(income_total, 0) = 0 THEN 0 ELSE round(bonus_total / income_total, 0) END AS bonus_margin,
        CASE WHEN coalesce(income_total, 0) = 0 THEN 0 ELSE round(profit_total / income_total, 0) END AS margin,
        CASE WHEN profit_total = 0 THEN 0 ELSE round(cost_total / profit_total, 2) END AS payback
    FROM intermediate_step_2_source
    LEFT JOIN bonus_cleverbox
        ON intermediate_step_2_source.eid = bonus_cleverbox.bonus_cleverbox_id
),

final AS (
    SELECT * FROM intermediate_step_3_source
)

{{ tf_transform_model('final') }}
