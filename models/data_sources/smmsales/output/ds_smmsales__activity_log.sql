WITH source AS (
    SELECT * FROM {{ tf_ref('ds_smmsales__parsed__activity_log_2501') }}
    UNION ALL
    SELECT * FROM {{ tf_ref('ds_smmsales__parsed__activity_log_2502') }}
    UNION ALL
    SELECT * FROM {{ tf_ref('ds_smmsales__parsed__activity_log_2503') }}
    UNION ALL
    SELECT * FROM {{ tf_ref('ds_smmsales__parsed__activity_log_2504') }}
    UNION ALL
    SELECT * FROM {{ tf_ref('ds_smmsales__parsed__activity_log_2505') }}
    UNION ALL
    SELECT * FROM {{ tf_ref('ds_smmsales__parsed__activity_log_2506') }}
),

final AS (
    SELECT
        source.*,
        IF(TRIM(status) = "Запис", 1, 0) AS booked
    FROM source
)

{{ tf_transform_model('final') }}
