WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__processed__visits') }}
)

{{ tf_transform_model('source') }}
