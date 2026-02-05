WITH employees_position AS (
    SELECT
        name AS employees_position_expert_name,
        job_title AS expert_position
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
    GROUP BY name, job_title
),

vip_clients_table AS (
    SELECT client_name AS vip_client_name
    FROM {{ tf_source('ds_cleverbox__raw__vip_clients') }}
),

employees AS (
    SELECT name_for_service
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
),

report_subscriptions_step_1 AS (
    SELECT
        *,
        cost - COALESCE(discount, 0) AS price,
        amount * cost AS cost_total,
        NOT COALESCE(vip_client_name IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_service IS NULL, FALSE) AS is_employee,
        amount * cost_price_unit AS cost_price_total,
        COALESCE(discount, 0) AS discount_total
    FROM {{ tf_ref('ds_cleverbox__parsed__subscription_sales') }} AS subscriptions_sales
    LEFT JOIN employees_position
        ON subscriptions_sales.expert_name = employees_position.employees_position_expert_name
    LEFT JOIN vip_clients_table
        ON subscriptions_sales.client = vip_clients_table.vip_client_name
    LEFT JOIN employees
        ON subscriptions_sales.client = employees.name_for_service
),

final AS (
    SELECT
        *,
        amount * price AS income_total
    FROM report_subscriptions_step_1
)

{{ tf_transform_model('final') }}
