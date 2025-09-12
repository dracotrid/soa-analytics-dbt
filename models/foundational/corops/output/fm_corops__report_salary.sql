WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__src__salary') }}
)

{{ tf_transform_model('source') }}
