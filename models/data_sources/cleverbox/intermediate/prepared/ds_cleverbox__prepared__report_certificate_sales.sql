WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__interm__report_certificate_sales') }}
)

{{ tf_transform_model('source') }}
