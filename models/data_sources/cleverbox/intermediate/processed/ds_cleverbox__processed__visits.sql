WITH final AS (
    SELECT
        * EXCEPT (visit_status),
        CASE
            WHEN visit_status IN ('Новий візит', 'Візит підтверджено', 'Попередне нагадування', 'Клієнт в салоні') THEN 'Новий візит'
            WHEN visit_status IN ('Відміна', 'Перенос', 'Передумала', 'Неявка', 'За сімейними обставинами') THEN 'Відміна'
            ELSE visit_status
        END AS visit_status
    FROM {{ tf_ref('ds_cleverbox__parsed__visits') }}
)

{{ tf_transform_model('final') }}
