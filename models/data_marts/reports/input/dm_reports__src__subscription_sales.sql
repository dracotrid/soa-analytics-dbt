WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__subscription_sales') }}
)

{{ tf_transform_model('source') }}
