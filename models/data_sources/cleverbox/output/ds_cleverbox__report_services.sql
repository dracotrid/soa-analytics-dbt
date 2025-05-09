WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__intermediate_step_2_report_services') }}
)

{{ tf_transform_model('source') }}
