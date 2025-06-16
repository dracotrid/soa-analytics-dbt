WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__report_balances') }}
)

{{ tf_transform_model('source') }}
