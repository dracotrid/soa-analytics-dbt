WITH employees_position AS (
    SELECT
        name AS employees_position_name,
        job_title AS expert_position
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
    GROUP BY name, job_title
),

services AS (
    SELECT
        code AS services_code,
        cost_price AS service_cost_price
    FROM {{ tf_ref('ds_cleverbox__parsed__services') }}
),

intermediate_step_1 AS (
    SELECT
        REPLACE(
            CONCAT(
                FORMAT_DATE('%Y-%m-%d', date),
                '__',
                branch,
                '__',
                code,
                '__',
                client_code,
                '__',
                expert_name,
                '__',
                discount,
                '__',
                subscription
            ),
            ' ',
            '_'
        ) AS eid,
        date,
        branch,
        code,
        name,
        SUM(1) AS amount,
        SUM(value) AS `value`,
        SUM(paid) AS paid,
        SUM(discount) AS discount,
        SUM(subscription) AS subscription,
        client_code,
        client_name, -- TODO: deprecate field usage and move to client list export (task SOA-60)
        expert_name,
        SUM(cost_price) AS cost_price,
        category,
        direction
    FROM {{ tf_ref('ds_cleverbox__parsed__service_sales') }}
    GROUP BY
        eid,
        date,
        branch,
        code,
        name,
        client_code,
        client_name,
        expert_name,
        category,
        direction
),

final AS (
    SELECT
        *,
        CASE
            WHEN EXTRACT(YEAR FROM date) = 2024 THEN cost_price
            WHEN COALESCE(service_cost_price, 0) = 0 THEN cost_price
            ELSE service_cost_price * amount
        END AS cost_price_total
    FROM intermediate_step_1 AS service_sales
    LEFT JOIN employees_position
        ON service_sales.expert_name = employees_position.employees_position_name
    LEFT JOIN services
        ON service_sales.code = services.services_code
)

{{ tf_transform_model('final') }}
