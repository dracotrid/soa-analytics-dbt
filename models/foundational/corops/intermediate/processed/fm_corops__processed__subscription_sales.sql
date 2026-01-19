WITH subscription_sales_step_1 AS (
    SELECT
        *,
        CASE WHEN cost_total = 0 THEN 0 ELSE 1 END AS payback,
        income_total - cost_price_total - bonus_total AS profit_total
    FROM {{ tf_ref('fm_corops__src__subscription_sales') }}
),

final AS (
    SELECT
        *,
        CASE WHEN income_total = 0 THEN 0 ELSE profit_total / income_total END AS margin
    FROM subscription_sales_step_1
)

{{ tf_transform_model('final') }}
