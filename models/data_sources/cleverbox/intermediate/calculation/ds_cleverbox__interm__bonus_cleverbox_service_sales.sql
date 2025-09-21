WITH service_sales AS (
    SELECT
        eid AS service_sales_id,
        cleverbox_cost_price_total
    FROM {{ tf_ref('ds_cleverbox__processed__service_sales') }}
),

intermediate_step_1 AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN bonus_value
            WHEN paid = 0 THEN 0
            WHEN is_bonus_without_cost_price = TRUE THEN paid * bonus_value
            ELSE (paid - cleverbox_cost_price_total) * bonus_value
        END AS bonus_cleverbox_total
    FROM {{ tf_ref('ds_cleverbox__interm__bonus_service_sales') }} AS bonus_report
    LEFT JOIN service_sales
        ON bonus_report.eid = service_sales.service_sales_id

),

intermediate_step_2 AS (
    SELECT
        *,
        bonus_cleverbox_total / amount AS bonus_cleverbox_unit
    FROM intermediate_step_1
),

final AS (
    SELECT * FROM intermediate_step_2
)



{{ tf_transform_model('final') }}
