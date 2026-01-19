WITH corops__bonus_goods_sales AS (
    SELECT
        eid AS corops_eid,
        base_for_bonus
    FROM {{ tf_ref('fm_corops__bonus_goods_sales') }}
),

source AS (
    SELECT *
    FROM {{ tf_ref('ds_cleverbox__bonus_goods_sales') }} AS cleverbox__bonus_goods_sales
    LEFT JOIN corops__bonus_goods_sales
        ON cleverbox__bonus_goods_sales.eid = corops__bonus_goods_sales.corops_eid
)

{{ tf_transform_model('source') }}
