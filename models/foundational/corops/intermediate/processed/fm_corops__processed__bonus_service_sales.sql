WITH service_sales AS (
    SELECT
        eid AS service_sales__eid,
        paid,
        income_total,
        cost_total AS cost,
        cost_price_total AS cost_price
    FROM {{ tf_ref('fm_corops__src__service_sales') }}
),

processed_step_1_source AS (
    SELECT
        *,
        ROUND(CASE
            WHEN bonus_type_for_calculation = '%ВідОплати' THEN paid
            WHEN bonus_type_for_calculation = '%ВідВартості' THEN cost
            WHEN bonus_type_for_calculation = 'Фіксована' THEN fixed_bonus_sum
            ELSE 0
        END, 2) AS cost_for_bonus
    FROM {{ tf_ref('fm_corops__src__bonus_service_sales') }} AS bonus_service_sales
    LEFT JOIN service_sales
        ON service_sales.service_sales__eid = bonus_service_sales.eid
),

processed_step_2_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN fixed_bonus_sum
            WHEN is_bonus_without_cost_price THEN cost_for_bonus
            ELSE cost_for_bonus - cost_price
        END AS base_for_bonus
    FROM processed_step_1_source
),

processed_step_3_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN fixed_bonus_sum
            ELSE base_for_bonus * bonus_percent
        END AS bonus_calculated,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN fixed_bonus_sum
            ELSE base_for_bonus * bonus_percent_test
        END AS bonus_calculated_test
    FROM processed_step_2_source
),

processed_step_4_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_calculated < 0 THEN 0
            ELSE bonus_calculated
        END AS bonus_total,
        CASE
            WHEN bonus_calculated_test < 0 THEN 0
            ELSE bonus_calculated_test
        END AS bonus_total_test
    FROM processed_step_3_source
),

final AS (
    SELECT * FROM processed_step_4_source
)

{{ tf_transform_model('final') }}
