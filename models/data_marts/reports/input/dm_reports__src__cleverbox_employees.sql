WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__employees') }}
)

{{ tf_transform_model('source') }}
