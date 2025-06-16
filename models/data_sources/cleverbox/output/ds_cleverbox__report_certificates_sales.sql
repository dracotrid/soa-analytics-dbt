WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__report_certificates_sales') }}
)

{{ tf_transform_model('source') }}
