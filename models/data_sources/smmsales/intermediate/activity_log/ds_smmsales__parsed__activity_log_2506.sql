WITH source AS (
    SELECT * FROM {{ tf_source('ds_smmsales__raw__activity_log_2506') }}
)

{{ tf_transform_model('source') }}
