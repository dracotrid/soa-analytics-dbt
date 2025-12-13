WITH source AS (
    SELECT
        'Послуга' AS product_type,
        date,
        paid AS income
    FROM {{ tf_ref('ds_cleverbox__interm__service_sales') }}

    UNION ALL

    SELECT
        'Товар' AS product_type,
        date,
        income_total AS income
    FROM {{ tf_ref('ds_cleverbox__prepared__goods_sales') }}

    UNION ALL

    SELECT
        'Абонемент' AS product_type,
        date,
        income_total AS income
    FROM {{ tf_ref('ds_cleverbox__prepared__subscription_sales') }}

    UNION ALL

    SELECT
        'Баланс' AS product_type,
        date,
        income_total AS income
    FROM {{ tf_ref('ds_cleverbox__prepared__balances') }}

)

{{ tf_transform_model('source') }}
