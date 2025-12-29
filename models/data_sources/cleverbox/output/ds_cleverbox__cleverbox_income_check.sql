WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__cleverbox_income_check') }}
)

{{ tf_transform_model('source') }}
