WITH bonuses AS (
    SELECT
        eid AS bonuses__eid,
        visit_number AS bonuses__visit_number,
        retro_bonus_total AS bonuses__retro_bonus_total,
        retro_bonus_visit AS bonuses__retro_bonus_visit,
        is_calc_retro_bonus AS bonuses__is_calc_retro_bonus
    FROM {{ tf_ref('fm_corops__processed__bonus_service_sales') }}
),

service_sales AS (
    SELECT *
    FROM {{ tf_ref('fm_corops__processed__service_sales') }} AS service_sales
    LEFT JOIN bonuses
        ON service_sales.eid = bonuses.bonuses__eid
),

service_sales_table_1 AS (
    SELECT
        date,
        client_code,
        bonuses__visit_number,
        direction,
        branch,
        ARRAY_AGG(DISTINCT expert_name)[SAFE_OFFSET(0)] AS expert_name
    FROM service_sales
    GROUP BY
        date,
        client_code,
        bonuses__visit_number,
        direction,
        branch
),

service_sales_table_2 AS (
    SELECT *
    FROM service_sales
    WHERE
        bonuses__retro_bonus_visit IS NOT NULL
),

retro_bonuses AS (
    SELECT
        t2.eid,
        t1.date AS lv_date,
        t2.date AS fv_date,
        t2.expert_name AS fv_expert_name,
        t1.expert_name AS lv_expert_name,
        t2.branch AS fv_branch,
        t1.branch AS lv_branch,
        t2.client_code,
        t2.client_name,
        t2.service_code,
        t2.service_name,
        t2.bonuses__retro_bonus_total AS retro_bonus_total,
        t1.bonuses__visit_number AS lv_visit_number,
        t2.bonuses__visit_number AS fv_visit_number,
        t2.bonuses__retro_bonus_visit AS fv_retro_bonus_visit
    FROM service_sales_table_1 AS t1
    LEFT JOIN service_sales_table_2 AS t2
        ON
            t1.client_code = t2.client_code
            AND t1.direction = t2.direction
            AND t1.bonuses__visit_number = t2.bonuses__retro_bonus_visit
            AND t2.bonuses__visit_number = 1
            AND t2.bonuses__is_calc_retro_bonus = TRUE
    WHERE t2.bonuses__retro_bonus_total IS NOT NULL
),

final AS (
    SELECT * FROM retro_bonuses
)

{{ tf_transform_model('final') }}
