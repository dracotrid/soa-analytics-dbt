WITH intermediate_step_1_source AS (
    SELECT
        *,
        CASE WHEN amount = 0 THEN 0 ELSE paid / amount END AS price,
        CASE WHEN amount = 0 THEN 0 ELSE cost_price_total / amount END AS cost_price_unit,
        CASE WHEN cost_total = 0 THEN 0 ELSE discount / cost_total * 100 END AS discount_percent,
        CASE
            WHEN subscription > 0 THEN 0
            WHEN paid < 1 THEN 0
            ELSE paid
        END AS income_total
    FROM {{ tf_ref('ds_cleverbox__processed__service_sales') }}
),

final AS (
    SELECT * FROM intermediate_step_1_source
)

{{ tf_transform_model('final') }}
