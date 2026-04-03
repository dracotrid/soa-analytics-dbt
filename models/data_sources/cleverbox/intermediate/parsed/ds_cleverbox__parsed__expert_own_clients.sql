WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__expert_own_clients') }}
)

{{ tf_transform_model('source') }}
