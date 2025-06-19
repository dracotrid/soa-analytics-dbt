WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__employees') }}
)

{{ tf_transform_model('source') }}
