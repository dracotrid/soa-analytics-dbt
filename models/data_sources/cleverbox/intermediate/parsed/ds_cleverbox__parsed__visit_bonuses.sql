WITH

visit_bonuses AS (
    SELECT
        3000 AS min_visit_sum,
        5000 AS max_visit_sum,
        50 AS visit_bonus,
        DATE('2026-01-01') AS visit_bonus_start_date

    UNION ALL

    SELECT
        5000 AS min_visit_sum,
        7000 AS max_visit_sum,
        75 AS visit_bonus,
        DATE('2026-01-01') AS visit_bonus_start_date

    UNION ALL

    SELECT
        7000 AS min_visit_sum,
        9000 AS max_visit_sum,
        100 AS visit_bonus,
        DATE('2026-01-01') AS visit_bonus_start_date

    UNION ALL

    SELECT
        9000 AS min_visit_sum,
        1000000 AS max_visit_sum,
        300 AS visit_bonus,
        DATE('2026-01-01') AS visit_bonus_start_date
),

setting AS (
    SELECT COALESCE(value = 'TRUE', FALSE) AS is_calculate
    FROM {{ tf_source('ds_cleverbox__raw__settings') }}
    WHERE name = 'Бонус візиту'
),

final AS (
    SELECT *
    FROM visit_bonuses,
        setting
)

{{ tf_transform_model('final') }}
