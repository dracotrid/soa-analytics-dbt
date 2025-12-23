WITH source AS (
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
)

{{ tf_transform_model('source') }}
