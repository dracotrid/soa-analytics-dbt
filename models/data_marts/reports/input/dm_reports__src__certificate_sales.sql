WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__certificate_sales') }}
)

{{ tf_transform_model('source') }}
