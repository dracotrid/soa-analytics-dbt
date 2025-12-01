WITH source AS (
    SELECT
        service_sales.eid,
        service_sales.date,
        'Послуга' AS product_type,
        service_sales.service_name AS product_name,
        service_sales.expert_name,
        service_sales.client_name,
        service_sales.direction,
        service_sales.amount,
        service_sales.cost_total AS cost,
        service_sales.discount_total AS discount,
        service_sales.price AS paid,
        service_sales.cost_price_total AS cost_price,
        internal_bonus.bonus_discount_name AS bonus_discount,
        internal_bonus.bonus_discount_type AS bonus_discount__bonus_type,
        internal_bonus.bonus_employee__code AS bonus_employee,
        service_sales.bonus_total AS internal_bonus,
        cleverbox_bonus.bonus_cleverbox_total AS cleverbox_bonus,
        internal_bonus.bonus_type_for_calculation AS internal_bonus_type,
        cleverbox_bonus.cleverbox_bonus_type,
        internal_bonus.bonus_value,
        internal_bonus.base_for_bonus AS base_for_internal_bonus,
        cleverbox_bonus.bonus_cleverbox_base_for_bonus AS base_for_cleverbox_bonus,
        internal_bonus.is_bonus_without_cost_price,
        internal_bonus.bonus_total - cleverbox_bonus.bonus_cleverbox_total AS delta
    FROM {{ tf_ref('ds_cleverbox__prepared__service_sales') }} AS service_sales
    LEFT JOIN {{ tf_ref('ds_cleverbox__interm__bonus_service_sales') }} AS internal_bonus
        ON service_sales.eid = internal_bonus.eid
    LEFT JOIN {{ tf_ref('ds_cleverbox__interm__bonus_cleverbox_service_sales') }} AS cleverbox_bonus
        ON service_sales.eid = cleverbox_bonus.eid

    UNION ALL

    SELECT
        goods_sales.eid,
        goods_sales.date,
        'Товар' AS product_type,
        goods_sales.goods_name AS product_name,
        goods_sales.expert_name,
        goods_sales.client_name,
        'Товар' AS direction,
        goods_sales.amount,
        goods_sales.cost_total AS cost,
        goods_sales.discount_total AS discount,
        bonus.paid,
        goods_sales.cost_price_total AS cost_price,
        bonus.bonus_discount_name AS bonus_discount,
        bonus.bonus_discount_type AS bonus_discount__bonus_type,
        bonus.bonus_employee_code AS bonus_employee,
        goods_sales.bonus_total AS internal_bonus,
        goods_sales.cleverbox_bonus_total AS cleverbox_bonus,
        bonus.bonus_type_for_calculation AS internal_bonus_type,
        bonus.cleverbox_bonus_type,
        bonus.bonus_value,
        bonus.base_for_bonus AS base_for_internal_bonus,
        bonus.cleverbox_base_for_bonus AS base_for_cleverbox_bonus,
        false AS is_bonus_without_cost_price,
        bonus.bonus_total - bonus.cleverbox_bonus_total AS delta
    FROM {{ tf_ref('ds_cleverbox__prepared__goods_sales') }} AS goods_sales
    LEFT JOIN {{ tf_ref('ds_cleverbox__interm__bonus_goods_sales') }} AS bonus
        ON goods_sales.eid = bonus.eid
)

{{ tf_transform_model('source') }}
