WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__balances') }}
)

{{ tf_transform_model('source') }}
