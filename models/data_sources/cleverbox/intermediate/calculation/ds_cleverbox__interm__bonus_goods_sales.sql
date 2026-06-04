WITH bonus_employee AS (
    SELECT
        uid AS bonus_employee_code,
        bonus_value AS bonus_employee_value,
        accrual_type AS bonus_employee_type,
        fixed_bonus_upon_payment AS bonus_employee__fixed_bonus_upon_payment,
        validity_from,
        validity_to
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

bonus_employee_values AS (
    SELECT
        eid AS bonus_employee_values__eid,
        CASE
            WHEN bonus_employee_name.bonus_employee_code IS NOT NULL THEN bonus_employee_name.bonus_employee_value
            WHEN bonus_employee_all.bonus_employee_code IS NOT NULL THEN bonus_employee_all.bonus_employee_value
        END AS bonus_employee_value,
        CASE
            WHEN bonus_employee_name.bonus_employee_code IS NOT NULL THEN bonus_employee_name.bonus_employee_type
            WHEN bonus_employee_all.bonus_employee_code IS NOT NULL THEN bonus_employee_all.bonus_employee_type
        END AS bonus_employee_type,
        CASE
            WHEN bonus_employee_name.bonus_employee_code IS NOT NULL THEN bonus_employee_name.bonus_employee__fixed_bonus_upon_payment
            WHEN bonus_employee_all.bonus_employee_code IS NOT NULL THEN bonus_employee_all.bonus_employee__fixed_bonus_upon_payment
        END AS bonus_employee__fixed_bonus_upon_payment,
        CASE
            WHEN bonus_employee_name.bonus_employee_code IS NOT NULL THEN bonus_employee_name.bonus_employee_code
            WHEN bonus_employee_all.bonus_employee_code IS NOT NULL THEN bonus_employee_all.bonus_employee_code
        END AS bonus_employee_code
    FROM {{ tf_ref('ds_cleverbox__processed__goods_sales') }} AS goods_sales
    LEFT JOIN bonus_employee AS bonus_employee_all
        ON
            CONCAT(goods_sales.expert_name, '-Товар-ВСЕ') = bonus_employee_all.bonus_employee_code
            AND (bonus_employee_all.validity_from IS NULL OR bonus_employee_all.validity_from <= date)
            AND (bonus_employee_all.validity_to IS NULL OR bonus_employee_all.validity_to >= date)
    LEFT JOIN bonus_employee AS bonus_employee_name
        ON
            CONCAT(goods_sales.expert_name, '-Товар-', TRIM(goods_sales.goods_name)) = bonus_employee_name.bonus_employee_code
            AND (bonus_employee_name.validity_from IS NULL OR bonus_employee_name.validity_from <= date)
            AND (bonus_employee_name.validity_to IS NULL OR bonus_employee_name.validity_to >= date)
),

bonus_report_goods_step_1 AS (
    SELECT
        *,
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
    LEFT JOIN bonus_employee_values
        ON goods_sales.eid = bonus_employee_values.bonus_employee_values__eid
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
            WHEN bonus_employee_type = 'Фіксована' THEN bonus_employee_type
            WHEN is_employee = TRUE THEN 'БезПремії'
            WHEN bonus_discount_type IS NOT NULL THEN bonus_discount_type
            WHEN is_vip = TRUE AND cost_total < 0 THEN '%ВідВартості'
            ELSE COALESCE(bonus_employee_type, '<<НЕВІДОМО>>')
        END AS bonus_type_for_calculation,
        paid AS cleverbox_base_for_bonus
    FROM bonus_report_goods_step_2
    LEFT JOIN bonus_discount
        ON bonus_report_goods_step_2.discount_usage_discount_name = bonus_discount.bonus_discount_name
),

bonus_report_goods_step_4 AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN 'Фіксована'
            ELSE '%ВідОплати'
        END AS cleverbox_bonus_type

    FROM bonus_report_goods_step_3
),

bonus_report_goods_step_5 AS (
    SELECT
        *,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN bonus_employee_value
            ELSE 0
        END AS fixed_bonus_sum,
        CASE
            WHEN bonus_type_for_calculation = 'Фіксована' THEN 0
            ELSE bonus_employee_value
        END AS bonus_percent
    FROM bonus_report_goods_step_4
),

final AS (
    SELECT
        *,
        CASE
            WHEN cleverbox_bonus_type = 'Фіксована' THEN fixed_bonus_sum
            ELSE ROUND(cleverbox_base_for_bonus * bonus_percent, 2)
        END AS cleverbox_bonus_total
    FROM bonus_report_goods_step_5
)

{{ tf_transform_model('final') }}
