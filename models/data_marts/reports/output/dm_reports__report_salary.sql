WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__prepared__salary') }}
)

{{ tf_transform_model('source') }}
