WITH source AS (
    SELECT
        eid,
        'Послуга' AS product_type,
        date,
        branch,
        expert_name,
        service_name AS product_name,
        bonus_total AS bonus,
        amount,
        bonus_cleverbox_total AS bonus_cleverbox
    FROM {{ tf_ref('dm_reports__prepared__service_sales') }}

    UNION ALL

    SELECT
        eid,
        'Товар' AS product_type,
        date,
        branch,
        expert_name,
        goods_name AS product_name,
        bonus_total AS bonus,
        amount,
        cleverbox_bonus_total AS bonus_cleverbox
    FROM {{ tf_ref('dm_reports__prepared__goods_sales') }}

    UNION ALL

    SELECT
        eid,
        'Бонус Ретро' AS product_type,
        date,
        branch,
        expert_name,
        service_name,
        retro_bonus_total AS bonus,
        1 AS amount,
        0 AS bonus_cleverbox
    FROM {{ tf_ref('dm_reports__src__retro_bonuses_service_sales') }}

    UNION ALL

    SELECT
        eid,
        'Бонус вiзит' AS product_type,
        date,
        branch,
        expert_name,
        CONCAT(date, '__', client_name, '_(', client_code, ')__', visit_sum, '__', service_sum, '__', goods_sum) AS service_name,
        visit_bonus AS bonus,
        1 AS amount,
        0 AS bonus_cleverbox
    FROM {{ tf_ref('dm_reports__src__direction_visits') }}
    WHERE visit_bonus IS NOT NULL

)

{{ tf_transform_model('source') }}
