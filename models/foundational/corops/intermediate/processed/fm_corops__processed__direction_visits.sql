WITH goods_sum AS (
    SELECT
        CONCAT(branch, client_name, date) AS goods_join_code,
        SUM(paid) AS paid
    FROM {{ tf_ref('fm_corops__processed__goods_sales') }}
    GROUP BY
        goods_join_code
),

service_sum AS (
    SELECT
        CONCAT(branch, direction, client_code, date) AS service_join_code,
        SUM(paid) AS paid
    FROM {{ tf_ref('fm_corops__processed__service_sales') }}
    GROUP BY
        service_join_code
),

bonus_service_sales AS (
    SELECT
        eid AS bonus__eid,
        visit_number
    FROM {{ tf_ref('fm_corops__processed__bonus_service_sales') }}

),

intermediate_step_1 AS (
    SELECT
        REPLACE(
            CONCAT(
                FORMAT_DATE('%Y-%m-%d', date),
                '__',
                branch,
                '__',
                direction,
                '__',
                client_code,
                '__',
                visit_number
            ),
            ' ',
            '_'
        ) AS eid,
        date,
        branch,
        direction,
        client_code,
        client_name,
        visit_number,
        expert_name
    FROM {{ tf_ref('fm_corops__src__service_sales') }} AS service_sales
    LEFT JOIN bonus_service_sales
        ON service_sales.eid = bonus_service_sales.bonus__eid
),

intermediate_step_2 AS (
    SELECT
        eid,
        date,
        branch,
        direction,
        client_code,
        client_name,
        visit_number,
        ANY_VALUE(expert_name) AS expert_name
    FROM intermediate_step_1
    GROUP BY
        eid,
        date,
        branch,
        direction,
        client_code,
        client_name,
        visit_number
),

intermediate_step_3 AS (
    SELECT
        *,
        COALESCE(goods_sum.paid, 0) + COALESCE(service_sum.paid, 0) AS visit_sum,
        COALESCE(goods_sum.paid, 0) AS goods_sum,
        COALESCE(service_sum.paid, 0) AS service_sum
    FROM intermediate_step_2
    LEFT JOIN service_sum
        ON CONCAT(branch, direction, client_code, date) = service_sum.service_join_code
    LEFT JOIN goods_sum
        ON CONCAT(branch, client_name, date) = goods_sum.goods_join_code
),

intermediate_step_4 AS (
    SELECT *
    FROM intermediate_step_3
    LEFT JOIN {{ tf_ref('fm_corops__src__visit_bonuses') }} AS visit_bonus
        ON
            visit_sum >= visit_bonus.min_visit_sum
            AND visit_sum < visit_bonus.max_visit_sum
            AND visit_number = 1
            AND (visit_bonus.visit_bonus_start_date IS NOT NULL AND visit_bonus.visit_bonus_start_date <= date)
            AND visit_bonus.is_calculate = TRUE
),

final AS (
    SELECT * FROM intermediate_step_4
)

{{ tf_transform_model('final') }}
