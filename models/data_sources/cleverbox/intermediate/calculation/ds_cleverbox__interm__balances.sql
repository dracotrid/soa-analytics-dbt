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

final AS (
    SELECT
        *,
        NOT COALESCE(vip_client_name IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_service IS NULL, FALSE) AS is_employee,
        amount * price AS cost_total,
        amount * price - discount AS income_total,
        amount * cost_price_unit AS cost_price_total
    FROM {{ tf_ref('ds_cleverbox__parsed__balances') }} AS balances
    LEFT JOIN employees_position
        ON balances.expert_name = employees_position.employees_position_expert_name
    LEFT JOIN vip_clients_table
        ON balances.client = vip_clients_table.vip_client_name
    LEFT JOIN employees
        ON balances.client = employees.name_for_service
)

{{ tf_transform_model('final') }}
