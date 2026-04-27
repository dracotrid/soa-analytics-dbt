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
        'Ретро бонус' AS product_type,
        date,
        branch,
        expert_name,
        service_name,
        retro_bonus_total AS bonus,
        1 AS amount,
        0 AS bonus_cleverbox
    FROM {{ tf_ref('dm_reports__src__retro_bonuses_service_sales') }}
)

{{ tf_transform_model('source') }}
