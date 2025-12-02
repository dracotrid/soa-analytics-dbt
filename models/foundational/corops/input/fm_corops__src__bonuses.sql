WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__bonuses') }}
)

{{ tf_transform_model('source') }}
