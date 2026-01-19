WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__prepared__service_sales') }}
)

{{ tf_transform_model('source') }}
