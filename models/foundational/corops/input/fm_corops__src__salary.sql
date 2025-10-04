WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__salary') }}
)

{{ tf_transform_model('source') }}
