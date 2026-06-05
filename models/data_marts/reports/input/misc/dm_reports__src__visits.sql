WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__visits') }}
)

{{ tf_transform_model('source') }}
