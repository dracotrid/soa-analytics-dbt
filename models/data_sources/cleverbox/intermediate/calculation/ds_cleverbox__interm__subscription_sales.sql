WITH employees_position AS (
    SELECT
        name AS employees_position_expert_name,
        job_title AS expert_position
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
        cost - COALESCE(discount, 0) AS price,
        CASE WHEN cost = 0 THEN 0 ELSE 1 END AS payback,
        amount * cost AS cost_total,
        NOT COALESCE(vip_clients IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_service IS NULL, FALSE) AS is_employee,
        amount * cost_price_unit AS cost_price_total,
        COALESCE(discount, 0) AS discount_total
    FROM {{ tf_ref('ds_cleverbox__parsed__subscriptions_sales') }} AS subscriptions_sales
    LEFT JOIN employees_position
        ON subscriptions_sales.expert_name = employees_position.employees_position_expert_name
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
        amount * price AS full_income
    FROM report_subscriptions_step_1
),

report_subscriptions_step_3 AS (
    SELECT
        *,
        full_income - COALESCE(certificates_balance_sum, 0) AS income_total
    FROM report_subscriptions_step_2
),

report_subscriptions_step_4 AS (
    SELECT
        *,
        income_total - cost_price_total - bonus_total AS profit_total
    FROM report_subscriptions_step_3
),

final AS (
    SELECT
        *,
        CASE WHEN full_income = 0 THEN 0 ELSE profit_total / full_income END AS margin
    FROM report_subscriptions_step_4
)

{{ tf_transform_model('final') }}
