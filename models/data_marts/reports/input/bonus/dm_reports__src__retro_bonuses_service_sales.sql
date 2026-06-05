WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__retro_bonuses_service_sales') }}
)

{{ tf_transform_model('source') }}
