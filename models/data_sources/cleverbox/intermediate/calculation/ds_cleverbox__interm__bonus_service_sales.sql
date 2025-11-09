WITH bonus_employee AS (
    SELECT
        uid AS code,
        bonus_value AS bonus_employee__value,
        accrual_type AS bonus_employee__type,
        use_cost_price AS bonus_employee__use_cost_price,
        min_bonus_first_visit AS bonus_employee__min_bonus_first_visit,
        expected_sum_first_visit AS bonus_employee__expected_sum_first_visit,
        validity_from,
        validity_to
    FROM {{ tf_ref('ds_cleverbox__parsed__bonus_employee') }}
),

goods_sum AS (
    SELECT
        CONCAT(expert_name, branch, client_name, date) AS goods_join_code,
        SUM(paid) AS paid
    FROM {{ tf_ref('ds_cleverbox__processed__goods_sales') }}
    GROUP BY
        goods_join_code
),

service_sum AS (
    SELECT
        CONCAT(expert_name, branch, client_name, date) AS service_join_code,
        SUM(paid) AS paid
    FROM {{ tf_ref('ds_cleverbox__processed__service_sales') }}
    GROUP BY
        service_join_code
),

bonus_employee_values AS (
    SELECT
        eid AS bonus_employee_values__eid,
        CASE
            WHEN is_first_visit AND visit_sum < bonus_employee__expected_sum_first_visit THEN bonus_employee__min_bonus_first_visit
            ELSE bonus_employee__value
        END AS bonus_employee__bonus_value,
        bonus_employee__type,
        bonus_employee__use_cost_price,
        bonus_employee__code,
        bonus_employee__value,
        bonus_employee__min_bonus_first_visit,
        bonus_employee__expected_sum_first_visit,
        is_first_visit,
        visit_sum
    FROM (
        SELECT
            eid,
            COALESCE(ROW_NUMBER() OVER (PARTITION BY client_code, direction ORDER BY date) = 1, FALSE) AS is_first_visit,
            CASE
                WHEN be__service_code.code IS NOT NULL THEN be__service_code.bonus_employee__value
                WHEN be__service_category.code IS NOT NULL THEN be__service_category.bonus_employee__value
                WHEN be__service_all.code IS NOT NULL THEN be__service_all.bonus_employee__value
            END AS bonus_employee__value,
            CASE
                WHEN be__service_code.code IS NOT NULL THEN be__service_code.bonus_employee__type
                WHEN be__service_category.code IS NOT NULL THEN be__service_category.bonus_employee__type
                WHEN be__service_all.code IS NOT NULL THEN be__service_all.bonus_employee__type
            END AS bonus_employee__type,
            CASE
                WHEN be__service_code.code IS NOT NULL THEN be__service_code.bonus_employee__use_cost_price
                WHEN be__service_category.code IS NOT NULL THEN be__service_category.bonus_employee__use_cost_price
                WHEN be__service_all.code IS NOT NULL THEN be__service_all.bonus_employee__use_cost_price
            END AS bonus_employee__use_cost_price,
            CASE
                WHEN be__service_code.code IS NOT NULL THEN be__service_code.bonus_employee__min_bonus_first_visit
                WHEN be__service_category.code IS NOT NULL THEN be__service_category.bonus_employee__min_bonus_first_visit
                WHEN be__service_all.code IS NOT NULL THEN be__service_all.bonus_employee__min_bonus_first_visit
            END AS bonus_employee__min_bonus_first_visit,
            CASE
                WHEN be__service_code.code IS NOT NULL THEN be__service_code.bonus_employee__expected_sum_first_visit
                WHEN be__service_category.code IS NOT NULL THEN be__service_category.bonus_employee__expected_sum_first_visit
                WHEN be__service_all.code IS NOT NULL THEN be__service_all.bonus_employee__expected_sum_first_visit
            END AS bonus_employee__expected_sum_first_visit,
            CASE
                WHEN be__service_code.code IS NOT NULL THEN be__service_code.code
                WHEN be__service_category.code IS NOT NULL THEN be__service_category.code
                WHEN be__service_all.code IS NOT NULL THEN be__service_all.code
            END AS bonus_employee__code,
            COALESCE(goods_sum.paid, 0) + COALESCE(service_sum.paid, 0) AS visit_sum
        FROM {{ tf_ref('ds_cleverbox__processed__service_sales') }} AS service_sales
        LEFT JOIN bonus_employee AS be__service_code
            ON
                CONCAT(service_sales.expert_name, '-Послуга-', LTRIM(service_sales.service_code, '0')) = be__service_code.code
                AND (be__service_code.validity_from IS NULL OR be__service_code.validity_from <= date)
                AND (be__service_code.validity_to IS NULL OR be__service_code.validity_to >= date)
        LEFT JOIN bonus_employee AS be__service_category
            ON
                CONCAT(service_sales.expert_name, '-Послуга-', service_sales.category) = be__service_category.code
                AND (be__service_category.validity_from IS NULL OR be__service_category.validity_from <= date)
                AND (be__service_category.validity_to IS NULL OR be__service_category.validity_to >= date)
        LEFT JOIN bonus_employee AS be__service_all
            ON
                CONCAT(service_sales.expert_name, '-Послуга-ВСЕ') = be__service_all.code
                AND (be__service_all.validity_from IS NULL OR be__service_all.validity_from <= date)
                AND (be__service_all.validity_to IS NULL OR be__service_all.validity_to >= date)
        LEFT JOIN service_sum
            ON CONCAT(expert_name, branch, client_name, date) = service_sum.service_join_code
        LEFT JOIN goods_sum
            ON CONCAT(expert_name, branch, client_name, date) = goods_sum.goods_join_code
    )
),

bonus_discount AS (
    SELECT
        name AS bonus_discount_name,
        accrual_type_service AS bonus_discount_type,
        bonus_service AS bonus_discount_value
    FROM {{ tf_ref('ds_cleverbox__parsed__bonus_discount') }}
),

discount_usage AS (
    SELECT
        eid AS discount_usage_id,
        discount_name AS discount_usage_discount_name
    FROM {{ tf_ref('ds_cleverbox__processed__discount_usage') }}
),

bonus_adjustment AS (
    SELECT
        yuid AS bonus_adjustment_id,
        bonus_type AS bonus_adjustment_type
    FROM {{ tf_ref('ds_cleverbox__parsed__bonus_adjustments') }}
),

vip_clients_table AS (
    SELECT client_name AS vip_client_name
    FROM {{ tf_source('ds_cleverbox__raw__vip_clients') }}
),

employees AS (
    SELECT name_for_service
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
),

intermediate_step_1_source AS (
    SELECT
        *,
        NOT COALESCE(vip_client_name IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_service IS NULL, FALSE) AS is_employee,
        CASE WHEN cost_total = 0 OR discount = 0 THEN 0 ELSE discount / cost_total END AS discount_rate
    FROM {{ tf_ref('ds_cleverbox__processed__service_sales') }} AS service_sales
    LEFT JOIN bonus_employee_values
        ON service_sales.eid = bonus_employee_values.bonus_employee_values__eid
    LEFT JOIN vip_clients_table
        ON service_sales.client_name = vip_clients_table.vip_client_name
    LEFT JOIN employees
        ON service_sales.client_name = employees.name_for_service
),

intermediate_step_2_source AS (
    SELECT
        *,
        CASE
            WHEN subscription > 0 THEN 'АБОНЕМЕНТ'
            WHEN discount_usage_id IS NULL THEN ''
            WHEN discount_usage_discount_name IS NULL OR discount_usage_discount_name = '' THEN 'АБОНЕМЕНТ'
            ELSE discount_usage_discount_name
        END AS discount_name_source
    FROM intermediate_step_1_source
    LEFT JOIN discount_usage
        ON intermediate_step_1_source.eid = discount_usage.discount_usage_id
),

intermediate_step_3_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_adjustment_type IS NOT NULL THEN bonus_adjustment_type
            WHEN bonus_employee__type IS NULL THEN 'БезПремії'
            WHEN bonus_employee__type = 'Фіксована' THEN bonus_employee__type
            WHEN is_employee = TRUE THEN '%ВідОплати'
            WHEN subscription IS NOT NULL AND subscription > 0 THEN bonus_employee__type
            -- '%ВідВартості' is used for VIPs and all the 100% discounts before 2025
            WHEN is_vip = TRUE OR (date < '2025-01-01' AND discount_rate = 1) THEN '%ВідВартості'
            WHEN bonus_discount_type IS NULL OR bonus_discount_type = '' THEN bonus_employee__type
            ELSE bonus_discount_type
        END AS bonus_type_for_calculation,
        COALESCE(bonus_discount_value, bonus_employee__bonus_value) AS bonus_value,
        year = '2024' OR NOT bonus_employee__use_cost_price AS is_bonus_without_cost_price
    FROM intermediate_step_2_source
    LEFT JOIN bonus_discount
        ON intermediate_step_2_source.discount_name_source = bonus_discount.bonus_discount_name
    LEFT JOIN bonus_adjustment
        ON intermediate_step_2_source.eid = bonus_adjustment.bonus_adjustment_id
),

intermediate_step_4_source AS (
    SELECT
        *,
        ROUND(CASE
            WHEN bonus_type_for_calculation = '%ВідОплати' THEN paid
            WHEN bonus_type_for_calculation = '%ВідВартості' THEN cost_total
            WHEN bonus_type_for_calculation = 'Фіксована' THEN bonus_value
            ELSE 0
        END, 2) AS cost_for_bonus
    FROM intermediate_step_3_source
),

intermediate_step_5_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN bonus_value
            WHEN is_bonus_without_cost_price THEN cost_for_bonus
            ELSE cost_for_bonus - cost_price_total
        END AS base_for_bonus
    FROM intermediate_step_4_source
),

intermediate_step_6_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN bonus_value
            ELSE base_for_bonus * bonus_value
        END AS bonus_calculated
    FROM intermediate_step_5_source
),

intermediate_step_7_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_calculated < 0 THEN 0
            ELSE bonus_calculated
        END AS bonus_total
    FROM intermediate_step_6_source
),

final AS (
    SELECT * FROM intermediate_step_7_source
)

{{ tf_transform_model('final') }}
