WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__prepared__balances') }}
)

{{ tf_transform_model('source') }}
