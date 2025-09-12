WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__src__balances') }}
)

{{ tf_transform_model('source') }}
