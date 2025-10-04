WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__balances') }}
)

{{ tf_transform_model('source') }}
