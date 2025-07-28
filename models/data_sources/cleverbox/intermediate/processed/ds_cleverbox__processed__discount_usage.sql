WITH discount_usage_step_1 AS (
    SELECT
        *,
        CASE
            WHEN COALESCE(discount_name, '') = '' THEN COALESCE(value, 0) - COALESCE(discount_value, 0)
            ELSE 0
        END AS subscription
    FROM {{ tf_ref('ds_cleverbox__parsed__discount_usage') }}

),

--TODO Add type STRING.CODE (numeric code with leading 0, length 6)
--TODO Add check and filter null values in id
discount_usage_step_2 AS (
    SELECT
        REPLACE(
            CONCAT(
                FORMAT_DATE('%Y-%m-%d', date),
                '__',
                'SoloMia Поділ',
                '__',
                COALESCE(product_id, ''),
                '__',
                COALESCE(client_code, ''),
                '__',
                COALESCE(expert_name, ''),
                '__',
                COALESCE(discount_value, 0),
                '__',
                COALESCE(subscription, 0)
            ),
            ' ',
            '_'
        ) AS eid,
        REPLACE(
            CONCAT(
                FORMAT_DATE('%Y-%m-%d', date),
                '__',
                'SoloMia Поділ',
                '__',
                COALESCE(product_name, ''),
                '__',
                COALESCE(client_name, ''),
                '__',
                COALESCE(expert_name, '')
            ),
            ' ',
            '_'
        ) AS goods_id,
        discount_name -- TODO if discount_name is empty set 'АБОНЕМЕНТ'
    FROM discount_usage_step_1
),

deduplicated AS (
    {{ dbt_utils.deduplicate(
        relation='discount_usage_step_2',
        partition_by='eid',
        order_by='discount_name'
    ) }}
)

{{ tf_transform_model('deduplicated') }}
