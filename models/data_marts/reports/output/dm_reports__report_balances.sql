WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__prepared__balances') }}
)

{{ tf_transform_model('source') }}
