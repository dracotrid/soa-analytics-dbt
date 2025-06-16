WITH service_sales AS (
    SELECT
        id AS service_sales_id,
        cost_price
    FROM {{ tf_ref('ds_cleverbox__processed__services_sales') }}
),

intermediate_step_1 AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN bonus_value
            WHEN paid = 0 THEN 0
            ELSE (paid - COALESCE(cost_price, 0)) * bonus_value
        END AS bonus_cleverbox_unit
    FROM {{ tf_ref('ds_cleverbox__interm__bonus_report_services_sales') }} AS bonus_report
    LEFT JOIN service_sales
        ON bonus_report.id = service_sales.service_sales_id

),

intermediate_step_2 AS (
    SELECT
        *,
        bonus_cleverbox_unit * amount AS bonus_cleverbox_total
    FROM intermediate_step_1
),

final AS (
    SELECT * FROM intermediate_step_2
)



{{ tf_transform_model('final') }}
