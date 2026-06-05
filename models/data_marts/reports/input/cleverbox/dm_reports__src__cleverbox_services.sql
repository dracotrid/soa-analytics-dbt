WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__services') }}
)

{{ tf_transform_model('source') }}
