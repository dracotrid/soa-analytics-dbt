WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__prepared__direction_visits') }}
)

{{ tf_transform_model('source') }}
