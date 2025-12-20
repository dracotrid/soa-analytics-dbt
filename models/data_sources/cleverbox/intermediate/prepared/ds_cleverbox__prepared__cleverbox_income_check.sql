WITH source AS (
    SELECT
        eid,
        'Послуга' AS product_type,
        date,
        paid AS income
    FROM {{ tf_ref('ds_cleverbox__interm__service_sales') }}

    UNION ALL

    SELECT
        eid,
        'Товар' AS product_type,
        date,
        income_total AS income
    FROM {{ tf_ref('ds_cleverbox__prepared__goods_sales') }}

    UNION ALL

    SELECT
        eid,
        'Абонемент' AS product_type,
        date,
        income_total AS income
    FROM {{ tf_ref('ds_cleverbox__prepared__subscription_sales') }}

    UNION ALL

    SELECT
        eid,
        'Баланс' AS product_type,
        date,
        income_total AS income
    FROM {{ tf_ref('ds_cleverbox__prepared__balances') }}

    UNION ALL

    SELECT
        eid,
        'Сертифікат' AS product_type,
        date,
        denomination AS income
    FROM {{ tf_ref('ds_cleverbox__prepared__certificate_sales') }}

)

{{ tf_transform_model('source') }}
