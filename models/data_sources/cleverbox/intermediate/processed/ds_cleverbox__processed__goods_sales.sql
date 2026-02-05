WITH vip_clients_table AS (
    SELECT client_name AS vip_client_name
    FROM {{ tf_source('ds_cleverbox__raw__vip_clients') }}
),

employees AS (
    SELECT name_for_goods
    FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
),

processed_step_1 AS (
    SELECT
        REPLACE(CONCAT(date, '__', branch, '__', receipt, '__', goods_name, '__', expert_name, '__', client_name), ' ', '_') AS eid,
        date,
        branch,
        receipt,
        expert_name,
        client_id,
        client_name,
        goods_name,
        brand,
        SUM(amount) AS amount,
        SUM(price * amount) AS paid,
        SUM(cost_price_unit * amount) AS cost_price_total,
        cost_price_unit,
        SUM(cost * amount) AS cost_total
    FROM {{ tf_ref('ds_cleverbox__parsed__goods_sales') }}
    GROUP BY
        eid,
        date,
        branch,
        receipt,
        expert_name,
        client_id,
        client_name,
        goods_name,
        brand,
        cost_price_unit
),

processed_step_2 AS (
    SELECT
        *,
        NOT COALESCE(vip_client_name IS NULL, FALSE) AS is_vip,
        NOT COALESCE(name_for_goods IS NULL, FALSE) AS is_employee
    FROM processed_step_1 AS goods_sales
    LEFT JOIN vip_clients_table
        ON goods_sales.client_name = vip_clients_table.vip_client_name
    LEFT JOIN employees
        ON goods_sales.client_name = employees.name_for_goods
),

source AS (
    SELECT * FROM processed_step_2
)

{{ tf_transform_model('source') }}
