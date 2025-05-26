WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__interm__report_goods') }}
)

{{ tf_transform_model('source') }}
