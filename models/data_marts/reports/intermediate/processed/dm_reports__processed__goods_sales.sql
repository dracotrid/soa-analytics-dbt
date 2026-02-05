WITH bonus_cleverbox_goods_sales AS (
    SELECT
        eid AS bonus_cleverbox_goods_sales__id,
        cleverbox_bonus_total
    FROM {{ tf_ref('dm_reports__src__cleverbox_bonus_goods_sales') }}
),

final AS (
    SELECT *
    FROM {{ tf_ref('dm_reports__src__goods_sales') }} AS goods_sales
    LEFT JOIN bonus_cleverbox_goods_sales
        ON goods_sales.eid = bonus_cleverbox_goods_sales.bonus_cleverbox_goods_sales__id
)

{{ tf_transform_model('final') }}
