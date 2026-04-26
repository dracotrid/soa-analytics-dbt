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
        cleverbox_service_sales.cleverbox_cost_price_total AS cleverbox_cost_price,
        cleverbox_internal_bonus.bonus_employee__code AS bonus_employee,
        service_sales.bonus_total AS internal_bonus,
        cleverbox_internal_bonus.bonus_cleverbox_total AS cleverbox_bonus,
        cleverbox_internal_bonus.bonus_type_for_calculation AS internal_bonus_type,
        cleverbox_internal_bonus.cleverbox_bonus_type,
        cleverbox_internal_bonus.bonus_percent,
        cleverbox_internal_bonus.fixed_bonus_sum,
        internal_bonus.base_for_bonus AS base_for_internal_bonus,
        cleverbox_internal_bonus.bonus_cleverbox_base_for_bonus AS base_for_cleverbox_bonus,
        cleverbox_internal_bonus.is_bonus_without_cost_price,
        service_sales.bonus_total - cleverbox_internal_bonus.bonus_cleverbox_total AS delta
    FROM {{ tf_ref('dm_reports__prepared__service_sales') }} AS service_sales
    LEFT JOIN {{ tf_ref('dm_reports__src__bonus_service_sales') }} AS internal_bonus
        ON service_sales.eid = internal_bonus.eid
    LEFT JOIN {{ tf_ref('dm_reports__src__cleverbox_service_sales') }} AS cleverbox_service_sales
        ON service_sales.eid = cleverbox_service_sales.eid
    LEFT JOIN {{ tf_ref('dm_reports__src__cleverbox_bonus_service_sales') }} AS cleverbox_internal_bonus
        ON service_sales.eid = cleverbox_internal_bonus.eid

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
        goods_sales.paid,
        goods_sales.cost_price_total AS cost_price,
        goods_sales.cost_price_total AS cleverbox_cost_price,
        bonus_cleverbox.bonus_employee_code AS bonus_employee,
        goods_sales.bonus_total AS internal_bonus,
        goods_sales.cleverbox_bonus_total AS cleverbox_bonus,
        bonus_cleverbox.bonus_type_for_calculation AS internal_bonus_type,
        bonus_cleverbox.cleverbox_bonus_type,
        bonus_cleverbox.bonus_percent,
        bonus_cleverbox.fixed_bonus_sum,
        bonus.base_for_bonus AS base_for_internal_bonus,
        bonus_cleverbox.cleverbox_base_for_bonus AS base_for_cleverbox_bonus,
        false AS is_bonus_without_cost_price,
        goods_sales.bonus_total - bonus_cleverbox.cleverbox_bonus_total AS delta
    FROM {{ tf_ref('dm_reports__prepared__goods_sales') }} AS goods_sales
    LEFT JOIN {{ tf_ref('dm_reports__src__cleverbox_bonus_goods_sales') }} AS bonus_cleverbox
        ON goods_sales.eid = bonus_cleverbox.eid
    LEFT JOIN {{ tf_ref('dm_reports__src__bonus_goods_sales') }} AS bonus
        ON goods_sales.eid = bonus.eid
)

{{ tf_transform_model('source') }}
