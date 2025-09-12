WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__output__salary') }}
)

{{ tf_transform_model('source') }}
