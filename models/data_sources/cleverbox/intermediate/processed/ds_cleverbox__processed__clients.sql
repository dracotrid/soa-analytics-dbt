WITH union_data AS (
    SELECT
        client_name,
        client_code
    FROM {{ tf_ref('ds_cleverbox__parsed__service_sales') }}

    UNION DISTINCT

    SELECT
        client_name,
        client_id AS client_code
    FROM {{ tf_ref('ds_cleverbox__parsed__goods_sales') }}
    WHERE client_id <> 'UNKNOWN_CLIENT_ID'
),

duplicate_names AS (
    SELECT client_name
    FROM union_data
    GROUP BY client_name
    HAVING COUNT(DISTINCT client_code) > 1
),

final AS (
    SELECT DISTINCT
        client_name,
        client_code
    FROM union_data
    WHERE client_name NOT IN (SELECT client_name FROM duplicate_names)
)

{{ tf_transform_model('final') }}
