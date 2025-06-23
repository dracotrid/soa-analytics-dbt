--- TODO: Calculation modules for Balances, Subscriptions and Certificates equals
--- TODO: Move it into one comon module

WITH employees_speciality AS (
    SELECT
        name AS employees_speciality_name,
        job_title AS speciality
    FROM {{ tf_ref('ds_cleverbox__processed__employees') }}
    GROUP BY name, job_title
),

vip_clients_table AS (
    SELECT vip_clients
    FROM {{ tf_source('ds_cleverbox__raw__clients') }}
),

employees AS (
    SELECT name_for_service
    FROM {{ tf_ref('ds_cleverbox__processed__employees') }}
),

certificates_sales_discount AS (
    SELECT
        uid AS discount_id,
        discount
    FROM {{ tf_source('ds_cleverbox__raw__certificates_sales_discount') }}
),

report_certificates_sales_step_1 AS (
    SELECT
        *,
        NOT COALESCE(vip_clients IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_service IS NULL, FALSE) AS is_employee,
        COALESCE(amount, 0) * COALESCE(price, 0) AS cost_total,
        COALESCE(amount, 0) * COALESCE(price, 0) - COALESCE(discount, 0) AS income_total,
        CASE WHEN COALESCE(price, 0) = 0 THEN 0 ELSE 1 END AS payback,
        COALESCE(amount, 0) * COALESCE(cost_price_unit, 0) AS cost_price_total
    FROM {{ tf_ref('ds_cleverbox__processed__certificates_sales') }} AS certificates_sales
    LEFT JOIN employees_speciality
        ON certificates_sales.employee = employees_speciality.employees_speciality_name
    LEFT JOIN certificates_sales_discount
        ON certificates_sales.id = certificates_sales_discount.discount_id
    LEFT JOIN vip_clients_table
        ON certificates_sales.client = vip_clients_table.vip_clients
    LEFT JOIN employees
        ON certificates_sales.client = employees.name_for_service
),

report_certificates_sales_step_2 AS (
    SELECT
        *,
        COALESCE(income_total, 0) - COALESCE(cost_price_total, 0) - COALESCE(bonus_total, 0) AS profit_total
    FROM report_certificates_sales_step_1
),

final AS (
    SELECT
        *,
        CASE WHEN COALESCE(profit_total, 0) = 0 THEN 0 ELSE COALESCE(income_total, 0) / COALESCE(profit_total, 0) END AS margin
    FROM report_certificates_sales_step_2
)

{{ tf_transform_model('final') }}
