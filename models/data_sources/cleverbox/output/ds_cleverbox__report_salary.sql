WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__report_salary') }}
)

{{ tf_transform_model('source') }}
