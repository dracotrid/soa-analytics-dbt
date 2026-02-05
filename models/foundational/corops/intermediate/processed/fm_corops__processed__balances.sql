WITH balances_step_1 AS (
    SELECT
        *,
        CASE WHEN price = 0 THEN 0 ELSE 1 END AS payback,
        income_total - cost_price_total - bonus_total AS profit_total
    FROM {{ tf_ref('fm_corops__src__balances') }}
),

final AS (
    SELECT
        *,
        CASE WHEN profit_total = 0 THEN 0 ELSE income_total / profit_total END AS margin
    FROM balances_step_1
)

{{ tf_transform_model('final') }}
