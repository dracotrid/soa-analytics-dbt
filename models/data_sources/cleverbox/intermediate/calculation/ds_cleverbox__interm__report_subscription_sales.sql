WITH employees_speciality AS (
    SELECT
        name AS employees_speciality_name,
        job_title AS speciality
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
    GROUP BY name, job_title
),

vip_clients_table AS (
    SELECT vip_clients
    FROM {{ tf_source('ds_cleverbox__raw__clients') }}
),

employees AS (
    SELECT name_for_service
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
),

subscriptions_sales_discount AS (
    SELECT
        uid AS discount_id,
        discount
    FROM {{ tf_source('ds_cleverbox__raw__subscriptions_sales_discount') }}
),

certificates_and_balance AS (
    SELECT
        uid AS certificates_balance_id,
        SUM(sum) AS certificates_balance_sum
    FROM {{ tf_source('ds_cleverbox__raw__certificates_and_balance_subscriptions') }}
    GROUP BY uid
),

report_subscriptions_step_1 AS (
    SELECT
        *,
        COALESCE(cost, 0) - COALESCE(discount, 0) AS price,
        CASE WHEN COALESCE(cost, 0) = 0 THEN 0 ELSE 1 END AS payback,
        COALESCE(amount, 0) * COALESCE(cost, 0) AS cost_total,
        NOT COALESCE(vip_clients IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_service IS NULL, FALSE) AS is_employee,
        COALESCE(amount, 0) * COALESCE(cost_price_unit, 0) AS cost_price_total,
        COALESCE(discount, 0) AS discount_total
    FROM {{ tf_ref('ds_cleverbox__parsed__subscriptions_sales') }} AS subscriptions_sales
    LEFT JOIN employees_speciality
        ON subscriptions_sales.employee = employees_speciality.employees_speciality_name
    LEFT JOIN subscriptions_sales_discount
        ON subscriptions_sales.id = subscriptions_sales_discount.discount_id
    LEFT JOIN certificates_and_balance
        ON subscriptions_sales.id = certificates_and_balance.certificates_balance_id
    LEFT JOIN vip_clients_table
        ON subscriptions_sales.client = vip_clients_table.vip_clients
    LEFT JOIN employees
        ON subscriptions_sales.client = employees.name_for_service
),

report_subscriptions_step_2 AS (
    SELECT
        *,
        COALESCE(amount, 0) * COALESCE(price, 0) AS full_income
    FROM report_subscriptions_step_1
),

report_subscriptions_step_3 AS (
    SELECT
        *,
        COALESCE(full_income, 0) - COALESCE(certificates_balance_sum, 0) AS income_total
    FROM report_subscriptions_step_2
),

report_subscriptions_step_4 AS (
    SELECT
        *,
        COALESCE(income_total, 0) - COALESCE(cost_price_total, 0) - COALESCE(bonus_total, 0) AS profit_total
    FROM report_subscriptions_step_3
),

final AS (
    SELECT
        *,
        CASE WHEN COALESCE(full_income, 0) = 0 THEN 0 ELSE COALESCE(profit_total, 0) / COALESCE(full_income, 0) END AS margin
    FROM report_subscriptions_step_4
)

{{ tf_transform_model('final') }}
