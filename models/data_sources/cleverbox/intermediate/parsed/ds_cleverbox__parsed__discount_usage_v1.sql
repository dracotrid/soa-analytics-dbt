WITH source AS (
    SELECT
        *,
        _FILE_NAME AS file_name
    FROM {{ tf_source('ds_cleverbox__raw__discount_usage') }}
)

{{ tf_transform_model('source') }}
