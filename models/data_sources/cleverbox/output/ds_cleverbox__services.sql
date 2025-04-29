WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__services') }}
)

{{ tf_transform_model('source') }}
