WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__services') }}
)

{{ tf_transform_model('source') }}
