WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__salary') }}
)

{{ tf_transform_model('source') }}
