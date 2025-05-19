WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__interm__income_report_services') }}
)

{{ tf_transform_model('source') }}
