WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__visits') }}
)

{{ tf_transform_model('source') }}
