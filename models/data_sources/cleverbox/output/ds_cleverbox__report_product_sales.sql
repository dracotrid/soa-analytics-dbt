WITH source AS (
    SELECT
        id,
        'Послуга' AS product_type,
        date,
        branch,
        specialist,
        client_name AS client,
        service_name AS product_name,
        direction,
        category,
        amount,
        price,
        cost_price_unit,
        payback,
        cost_total AS cost,
        income_total AS income,
        cost_price_total AS cost_price,
        discount_total AS discount,
        bonus_total AS bonus,
        profit_total AS profit,
        margin
    FROM {{ tf_ref('ds_cleverbox__report_services') }}

    UNION ALL

    SELECT
        id,
        'Товар' AS product_type,
        date,
        branch,
        specialist,
        client_name AS client,
        goods_name AS product_name,
        'Товар' AS direction,
        brand AS category,
        amount,
        price,
        cost_price_unit,
        payback,
        cost_total AS cost,
        income_total AS income,
        cost_price_total AS cost_price,
        discount_total AS discount,
        bonus_total AS bonus,
        profit_total AS profit,
        margin
    FROM {{ tf_ref('ds_cleverbox__report_goods') }}

    UNION ALL

    SELECT
        id,
        'Абонемент' AS product_type,
        date,
        branch,
        specialist,
        client,
        subscription_name AS product_name,
        direction,
        category,
        amount,
        price,
        cost_price_unit,
        payback,
        cost_total AS cost,
        income_total AS income,
        cost_price_total AS cost_price,
        discount_total AS discount,
        bonus_total AS bonus,
        profit_total AS profit,
        margin
    FROM {{ tf_ref('ds_cleverbox__report_subscriptions') }}

    UNION ALL

    SELECT
        id,
        'Сертифікат' AS product_type,
        date,
        branch,
        specialist,
        client,
        certificate_name AS product_name,
        direction,
        category,
        amount,
        price,
        cost_price_unit,
        payback,
        cost_total AS cost,
        income_total AS income,
        cost_price_total AS cost_price,
        discount,
        bonus_total AS bonus,
        profit_total AS profit,
        margin
    FROM {{ tf_ref('ds_cleverbox__report_certificates_sales') }}

    UNION ALL

    SELECT
        id,
        'Баланс' AS product_type,
        date,
        branch,
        specialist,
        client,
        name AS product_name,
        direction,
        category,
        amount,
        price,
        cost_price_unit,
        payback,
        cost_total AS cost,
        income_total AS income,
        cost_price_total AS cost_price,
        discount,
        bonus_total AS bonus,
        profit_total AS profit,
        margin
    FROM {{ tf_ref('ds_cleverbox__report_balances') }}

)

{{ tf_transform_model('source') }}
