WITH source AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__parsed__visit_bonuses') }}
)

{{ tf_transform_model('source') }}
