WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__prepared__incorrect_visits') }}
)

{{ tf_transform_model('source') }}
