WITH v_count AS (
    SELECT
        CONCAT(client_name, '_', date, '_', direction) AS join_id,
        *
    FROM (
        SELECT
            client_name,
            date,
            direction,
            COUNT(*) AS v_count,
            STRING_AGG(expert_name) AS experts
        FROM (
            SELECT DISTINCT
                client_name,
                date,
                expert_name,
                direction
            FROM {{ tf_ref('dm_reports__prepared__service_sales') }}
        )
        GROUP BY client_name, date, direction
    ) AS m
    WHERE v_count > 1
    ORDER BY date DESC
),

expert_service AS (
    SELECT
        CONCAT(client_name, '_', date, '_', direction) AS join_id,
        expert_name,
        service_name
    FROM {{ tf_ref('dm_reports__prepared__service_sales') }}
),

final AS (
    SELECT
        vc.client_name,
        vc.date,
        vc.direction,
        vc.v_count,
        es.expert_name,
        es.service_name
    FROM v_count AS vc
    LEFT JOIN expert_service AS es
        ON vc.join_id = es.join_id
    ORDER BY vc.date DESC
)

{{ tf_transform_model('final') }}
