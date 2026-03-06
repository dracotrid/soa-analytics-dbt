WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__employees') }}
)

{{ tf_transform_model('source') }}
