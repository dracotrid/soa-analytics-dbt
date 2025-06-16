WITH source AS (
    SELECT
        id AS eid,
        'Послуга' AS product_type,
        date,
        branch,
        specialist AS employee_name,
        client_name,
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
    FROM {{ tf_ref('ds_cleverbox__report_services_sales') }}

    UNION ALL

    SELECT
        id AS eid,
        'Товар' AS product_type,
        date,
        branch,
        specialist AS employee_name,
        client_name,
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
    FROM {{ tf_ref('ds_cleverbox__report_goods_sales') }}

    UNION ALL

    SELECT
        id AS eid,
        'Абонемент' AS product_type,
        date,
        branch,
        specialist AS employee_name,
        client AS client_name,
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
    FROM {{ tf_ref('ds_cleverbox__report_subscriptions_sales') }}

    UNION ALL

    SELECT
        id AS eid,
        'Сертифікат' AS product_type,
        date,
        branch,
        specialist AS employee_name,
        client AS client_name,
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
        id AS eid,
        'Баланс' AS product_type,
        date,
        branch,
        specialist AS employee_name,
        client AS client_name,
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
