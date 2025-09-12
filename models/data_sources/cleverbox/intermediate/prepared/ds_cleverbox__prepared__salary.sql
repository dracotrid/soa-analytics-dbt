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
    FROM {{ tf_ref('ds_cleverbox__prepared__service_sales') }}

    UNION ALL

    SELECT
        eid,
        'Товар' AS product_type,
        date,
        branch,
        specialist AS expert_name,
        goods_name AS product_name,
        bonus_total AS bonus,
        amount,
        cleverbox_bonus_total AS bonus_cleverbox
    FROM {{ tf_ref('ds_cleverbox__prepared__goods_sales') }}
)

{{ tf_transform_model('source') }}
