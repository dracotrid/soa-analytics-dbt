WITH vip_clients_table AS (
    SELECT vip_clients
    FROM {{ tf_source('ds_cleverbox__raw__clients') }}
),

employees AS (
    SELECT name_for_goods
    FROM {{ tf_ref('ds_cleverbox__prepared__employees') }}
),

bonus_employee AS (
    SELECT
        uid AS bonus_employee_code,
        bonus_value AS bonus_employee_value,
        accrual_type AS bonus_employee_type
    FROM {{ tf_ref('ds_cleverbox__prepared__bonus_employee') }}
    WHERE sale_type = 'Товар'
),

discount_usage AS (
    SELECT
        ruid AS discount_usage_id,
        discount_name AS discount_usage_discount_name
    FROM {{ tf_ref('ds_cleverbox__prepared__discount_usage') }}
    GROUP BY ruid, discount_name
),

bonus_discount AS (
    SELECT
        name AS bonus_discount_name,
        accrual_type_goods AS bonus_discount_type,
        bonus_goods AS bonus_discount_value
    FROM {{ tf_ref('ds_cleverbox__prepared__bonus_discount') }}
),

bonus_report_goods_step_1 AS (
    SELECT
        *,
        NOT COALESCE(vip_clients IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_goods IS NULL, FALSE) AS is_employee,
        CONCAT('DISCOUNT++', FORMAT_DATE('%d.%m.%Y', date), '--', goods_name, '--', employee, '--', client_name) AS druid
    FROM {{ tf_ref('ds_cleverbox__prepared__goods_sales') }} AS goods_sales
    LEFT JOIN vip_clients_table
        ON goods_sales.client_name = vip_clients_table.vip_clients
    LEFT JOIN employees
        ON goods_sales.client_name = employees.name_for_goods
    LEFT JOIN bonus_employee
        ON CONCAT(goods_sales.employee, '-Товар-ВСЕ') = bonus_employee.bonus_employee_code
),

bonus_report_goods_step_2 AS (
    SELECT *
    FROM bonus_report_goods_step_1
    LEFT JOIN discount_usage
        ON bonus_report_goods_step_1.druid = discount_usage.discount_usage_id
),

bonus_report_goods_step_3 AS (
    SELECT
        *,
        CASE
            WHEN is_employee = TRUE THEN 'БезПремії'
            WHEN is_vip = TRUE AND cost < 0 THEN '%ВідВартості'
            ELSE NULLIF(bonus_employee_type, '<<НЕВІДОМО>>')
        END AS bonus_type_for_calculation
    FROM bonus_report_goods_step_2
    LEFT JOIN bonus_discount
        ON bonus_report_goods_step_2.discount_usage_discount_name = bonus_discount.bonus_discount_name
),

bonus_report_goods_step_4 AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = '%ВідОплати' THEN COALESCE(price, 0)
            WHEN bonus_type_for_calculation = '%ВідВартості' THEN COALESCE(cost, 0)
            WHEN bonus_type_for_calculation = 'Фіксована' THEN COALESCE(bonus_employee_value, 0)
            ELSE 0
        END AS base_for_bonus,
        CASE
            WHEN COALESCE(bonus_discount_value, 0) = 0 THEN COALESCE(bonus_employee_value, 0)
            ELSE COALESCE(bonus_discount_value, 0)
        END AS bonus_value
    FROM bonus_report_goods_step_3
),

bonus_report_goods_step_5 AS (
    SELECT
        *,
        base_for_bonus * bonus_value AS bonus_unit
    FROM bonus_report_goods_step_4
),

final AS (
    SELECT
        *,
        ROUND(bonus_unit * amount, 2) AS bonus_total,
        ROUND(COALESCE(price, 0) * bonus_value * amount, 2) AS cleverbox_bonus_total
    FROM bonus_report_goods_step_5
)

{{ tf_transform_model('final') }}
