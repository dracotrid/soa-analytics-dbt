WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__bonus_employee') }}
)

{{ tf_transform_model('source') }}
