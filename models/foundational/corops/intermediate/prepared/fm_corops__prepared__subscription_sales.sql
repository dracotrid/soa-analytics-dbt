WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__processed__subscription_sales') }}
)

{{ tf_transform_model('source') }}
