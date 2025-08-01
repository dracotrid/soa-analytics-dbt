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

report_balance_step_1 AS (
    SELECT
        *,
        NOT COALESCE(vip_clients IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_service IS NULL, FALSE) AS is_employee,
        amount * price AS cost_total,
        amount * price - discount AS income_total,
        CASE WHEN price = 0 THEN 0 ELSE 1 END AS payback,
        amount * cost_price_unit AS cost_price_total
    FROM {{ tf_ref('ds_cleverbox__parsed__balances') }} AS balances
    LEFT JOIN employees_speciality
        ON balances.specialist = employees_speciality.employees_speciality_name
    LEFT JOIN vip_clients_table
        ON balances.client = vip_clients_table.vip_clients
    LEFT JOIN employees
        ON balances.client = employees.name_for_service
),

report_balance_step_2 AS (
    SELECT
        *,
        income_total - cost_price_total - bonus_total AS profit_total
    FROM report_balance_step_1
),

final AS (
    SELECT
        *,
        CASE WHEN profit_total = 0 THEN 0 ELSE income_total / profit_total END AS margin
    FROM report_balance_step_2
)

{{ tf_transform_model('final') }}
