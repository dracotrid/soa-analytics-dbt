WITH clients AS (
    SELECT
        client_code AS clients__client_code,
        client_name AS clients__client_name
    FROM {{ tf_ref('ds_cleverbox__processed__clients') }}
),

enriched_goods_sales AS (
    SELECT
        *,
        CASE
            WHEN client_id IS NULL OR client_id = 'UNKNOWN_CLIENT_ID' THEN COALESCE(clients__client_code, client_name)
            ELSE client_id
        END AS client_code
    FROM {{ tf_ref('ds_cleverbox__parsed__goods_sales') }} AS goods_sales
    LEFT JOIN clients
        ON goods_sales.client_name = clients.clients__client_name
),

source AS (
    SELECT
        REPLACE(CONCAT(date, '__', branch, '__', receipt, '__', goods_name, '__', expert_name, '__', client_code), ' ', '_') AS eid,
        date,
        branch,
        receipt,
        expert_name,
        client_code,
        client_name,
        goods_name,
        brand,
        SUM(amount) AS amount,
        SUM(price * amount) AS paid,
        SUM(cost_price_unit * amount) AS cost_price_total,
        cost_price_unit,
        SUM(cost * amount) AS cost_total
    FROM enriched_goods_sales
    GROUP BY
        eid,
        date,
        branch,
        receipt,
        expert_name,
        client_code,
        client_name,
        goods_name,
        brand,
        cost_price_unit
)

{{ tf_transform_model('source') }}
