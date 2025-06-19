WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__interm__income_report_service_sales') }}
)

{{ tf_transform_model('source') }}
