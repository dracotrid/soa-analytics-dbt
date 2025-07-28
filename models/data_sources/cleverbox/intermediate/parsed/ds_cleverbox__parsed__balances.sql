WITH source AS (
    SELECT
        REPLACE(CONCAT('BALANCE__', date, '__', type, '__', author, '__', client, '__', sum), ' ', '_') AS eid,
        DATE(PARSE_DATETIME('%d.%m.%Y %H:%M', date)) AS `date`,
        type AS `name`,
        author AS specialist,
        client,
        SUM(1) AS amount,
        SUM(sum) AS price
    FROM {{ tf_source('ds_cleverbox__raw__balances') }}
    GROUP BY
        eid,
        `date`,
        `name`,
        specialist,
        client
)

{{ tf_transform_model('source') }}
