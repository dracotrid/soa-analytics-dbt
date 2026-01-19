WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__src__balances') }}
)

{{ tf_transform_model('source') }}
