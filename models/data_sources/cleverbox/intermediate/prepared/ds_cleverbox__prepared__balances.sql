WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__interm__balances') }}
)

{{ tf_transform_model('source') }}
