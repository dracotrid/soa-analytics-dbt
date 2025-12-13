WITH source AS (
    SELECT * FROM {{ tf_ref('fm_corops__scr__cleverbox_income_check') }}
)

{{ tf_transform_model('source') }}
