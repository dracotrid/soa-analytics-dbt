WITH vip_clients_table AS (
    SELECT client_name AS vip_client_name
    FROM {{ tf_source('ds_cleverbox__raw__vip_clients') }}
),

employees AS (
    SELECT name_for_goods
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
),

bonus_employee AS (
    SELECT
        uid AS bonus_employee_code,
        bonus_value AS bonus_employee_value,
        accrual_type AS bonus_employee_type
    FROM {{ tf_ref('ds_cleverbox__parsed__bonus_employee') }}
    WHERE sale_type = 'Товар'
),

discount_usage AS (
    SELECT
        goods_id AS discount_usage_id,
        discount_name AS discount_usage_discount_name
    FROM {{ tf_ref('ds_cleverbox__processed__discount_usage') }}
),

bonus_discount AS (
    SELECT
        name AS bonus_discount_name,
        accrual_type_goods AS bonus_discount_type,
        bonus_goods AS bonus_discount_value
    FROM {{ tf_ref('ds_cleverbox__parsed__bonus_discount') }}
),

bonus_report_goods_step_1 AS (
    SELECT
        *,
        NOT COALESCE(vip_client_name IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_goods IS NULL, FALSE) AS is_employee,
        REPLACE(
            CONCAT(
                FORMAT_DATE('%Y-%m-%d', date),
                '__',
                branch,
                '__',
                goods_name,
                '__',
                client_name,
                '__',
                expert_name
            ),
            ' ',
            '_'
        ) AS du_id
    FROM {{ tf_ref('ds_cleverbox__processed__goods_sales') }} AS goods_sales
    LEFT JOIN vip_clients_table
        ON goods_sales.client_name = vip_clients_table.vip_client_name
    LEFT JOIN employees
        ON goods_sales.client_name = employees.name_for_goods
    LEFT JOIN bonus_employee
        ON CONCAT(goods_sales.expert_name, '-Товар-ВСЕ') = bonus_employee.bonus_employee_code
),

bonus_report_goods_step_2 AS (
    SELECT *
    FROM bonus_report_goods_step_1
    LEFT JOIN discount_usage
        ON bonus_report_goods_step_1.du_id = discount_usage.discount_usage_id
),

bonus_report_goods_step_3 AS (
    SELECT
        *,
        CASE
            WHEN is_employee = TRUE THEN 'БезПремії'
            WHEN is_vip = TRUE AND cost_total < 0 THEN '%ВідВартості'
            ELSE (bonus_employee_type, '<<НЕВІДОМО>>')
        END AS bonus_type_for_calculation,
        '%ВідОплати' AS cleverbox_bonus_type,
        paid AS cleverbox_base_for_bonus
    FROM bonus_report_goods_step_2
    LEFT JOIN bonus_discount
        ON bonus_report_goods_step_2.discount_usage_discount_name = bonus_discount.bonus_discount_name
),

bonus_report_goods_step_4 AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = '%ВідОплати' THEN paid
            WHEN bonus_type_for_calculation = '%ВідВартості' THEN cost_total
            WHEN bonus_type_for_calculation = 'Фіксована' THEN bonus_employee_value
            ELSE 0
        END AS base_for_bonus,
        CASE
            WHEN COALESCE(bonus_discount_value, 0) = 0 THEN bonus_employee_value
            ELSE bonus_discount_value
        END AS bonus_value
    FROM bonus_report_goods_step_3
),

final AS (
    SELECT
        *,
        base_for_bonus * bonus_value AS bonus_total,
        ROUND(paid * bonus_value, 2) AS cleverbox_bonus_total
    FROM bonus_report_goods_step_4
)

{{ tf_transform_model('final') }}
