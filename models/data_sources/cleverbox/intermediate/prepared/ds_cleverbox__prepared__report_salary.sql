WITH source AS (
    SELECT
        id,
        'Послуга' AS product_type,
        date,
        branch,
        specialist,
        service_name AS product_name,
        bonus_total AS bonus,
        amount,
        bonus_cleverbox_total AS bonus_cleverbox
    FROM {{ tf_ref('ds_cleverbox__report_service_sales') }}

    UNION ALL

    SELECT
        id,
        'Товар' AS product_type,
        date,
        branch,
        specialist,
        goods_name AS product_name,
        bonus_total AS bonus,
        amount,
        cleverbox_bonus_total AS bonus_cleverbox
    FROM {{ tf_ref('ds_cleverbox__report_goods_sales') }}
)

{{ tf_transform_model('source') }}
