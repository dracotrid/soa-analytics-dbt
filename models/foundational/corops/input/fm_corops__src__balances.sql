WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__output__balances') }}
)

{{ tf_transform_model('source') }}
