WITH source AS (
    SELECT * FROM {{ tf_source('ds_cleverbox__raw__bonus_adjustments') }}
)

{{ tf_transform_model('source') }}
