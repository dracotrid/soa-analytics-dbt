WITH source AS (
    SELECT * FROM {{ tf_ref('dm_reports__prepared__cleverbox_income_check') }}
)

{{ tf_transform_model('source') }}
