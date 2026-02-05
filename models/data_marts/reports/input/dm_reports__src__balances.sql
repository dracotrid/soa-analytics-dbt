WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__balances') }}
)

{{ tf_transform_model('source') }}
