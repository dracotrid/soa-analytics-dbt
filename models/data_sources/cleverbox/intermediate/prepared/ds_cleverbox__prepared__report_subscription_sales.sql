WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__interm__report_subscription_sales') }}
)

{{ tf_transform_model('source') }}
