WITH bonus_employee_service_code AS (
    SELECT uid AS bonus_service_code_code
    FROM {{ tf_ref('ds_cleverbox__parsed__bonus_employee') }}
),

bonus_employee_service_category AS (
    SELECT uid AS bonus_service_category_code
    FROM {{ tf_ref('ds_cleverbox__parsed__bonus_employee') }}
),

bonus_employee_service_all AS (
    SELECT uid AS bonus_service_all_code
    FROM {{ tf_ref('ds_cleverbox__parsed__bonus_employee') }}
),

bonus_employee AS (
    SELECT
        uid AS bonus_employee_code,
        bonus_value AS bonus_employee_value,
        accrual_type AS bonus_employee_type
    FROM {{ tf_ref('ds_cleverbox__parsed__bonus_employee') }}
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
    SELECT vip_clients
    FROM {{ tf_source('ds_cleverbox__raw__clients') }}
),

employees AS (
    SELECT name_for_service
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
),

intermediate_step_1_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_service_code_code IS NOT NULL THEN bonus_service_code_code
            WHEN bonus_service_category_code IS NOT NULL THEN bonus_service_category_code
            WHEN bonus_service_all_code IS NOT NULL THEN bonus_service_all_code
        END AS expert_bonus_code,
        NOT COALESCE(vip_clients IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_service IS NULL, FALSE) AS is_employee,
        CASE WHEN cost_total = 0 OR discount = 0 THEN 0 ELSE discount / cost_total END AS discount_rate
    FROM {{ tf_ref('ds_cleverbox__processed__report_service_sales') }} AS service_sales
    LEFT JOIN bonus_employee_service_code
        ON CONCAT(service_sales.expert_name, '-Послуга-', service_sales.service_code) = bonus_employee_service_code.bonus_service_code_code
    LEFT JOIN bonus_employee_service_category
        ON CONCAT(service_sales.expert_name, '-Послуга-', service_sales.category) = bonus_employee_service_category.bonus_service_category_code
    LEFT JOIN bonus_employee_service_all
        ON CONCAT(service_sales.expert_name, '-Послуга-ВСЕ') = bonus_employee_service_all.bonus_service_all_code
    LEFT JOIN vip_clients_table
        ON service_sales.client_name = vip_clients_table.vip_clients
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
        END AS discount_name_source,
        COALESCE(year = '2024' OR expert_position IN ('Манікюр', 'Подолог'), FALSE) AS is_bonus_without_cost_price
    FROM intermediate_step_1_source
    LEFT JOIN discount_usage
        ON intermediate_step_1_source.eid = discount_usage.discount_usage_id
),

intermediate_step_3_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_adjustment_type IS NOT NULL THEN bonus_adjustment_type
            WHEN bonus_employee_type IS NULL THEN 'БезПремії'
            WHEN bonus_employee_type = 'Фіксована' THEN bonus_employee_type
            WHEN is_employee = TRUE THEN '%ВідОплати'
            WHEN subscription IS NOT NULL AND subscription > 0 THEN bonus_employee_type
            -- '%ВідВартості' is used for VIPs and all the 100% discounts before 2025
            WHEN is_vip = TRUE OR (date < '2025-01-01' AND discount_rate = 1) THEN '%ВідВартості'
            WHEN bonus_discount_type IS NULL OR bonus_discount_type = '' THEN bonus_employee_type
            ELSE bonus_discount_type
        END AS bonus_type_for_calculation,
        CASE
            WHEN expert_name = 'Могалова Надія' AND date < '2025-08-16' THEN 0.3 -- FIXEME SOA-77
            ELSE COALESCE(bonus_discount_value, bonus_employee_value)
        END AS bonus_value
    FROM intermediate_step_2_source
    LEFT JOIN bonus_employee
        ON intermediate_step_2_source.expert_bonus_code = bonus_employee.bonus_employee_code
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
        END AS bonus_total
    FROM intermediate_step_5_source
),

final AS (
    SELECT * FROM intermediate_step_6_source
)

{{ tf_transform_model('final') }}
