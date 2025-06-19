WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__bonus_discount') }}
)

{{ tf_transform_model('source') }}
