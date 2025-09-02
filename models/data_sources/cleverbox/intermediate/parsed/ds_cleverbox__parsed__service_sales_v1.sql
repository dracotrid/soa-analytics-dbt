WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__service_sales_enriched_raw') }}
)

{{ tf_transform_model('source') }}
