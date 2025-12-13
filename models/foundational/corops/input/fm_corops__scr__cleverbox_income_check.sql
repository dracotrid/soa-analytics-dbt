WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__cleverbox_income_check') }}
)

{{ tf_transform_model('source') }}
