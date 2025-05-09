WITH employees_speciality AS (
    SELECT
        name AS employees_speciality_name,
        job_title AS speciality
    FROM {{ tf_ref('ds_cleverbox__employees') }}
    GROUP BY name, job_title
),

services AS (
    SELECT
        guid,
        category AS service_category,
        direction AS service_direction,
        cost_price AS service_cost_price
    FROM {{ tf_ref('ds_cleverbox__services') }}
),

bonus_service_code AS (
    SELECT uid AS bonus_service_code_code
    FROM {{ tf_source('ds_cleverbox__raw__bonus_employee') }}
),

bonus_service_category AS (
    SELECT uid AS bonus_service_category_code
    FROM {{ tf_source('ds_cleverbox__raw__bonus_employee') }}
),

bonus_service_all AS (
    SELECT uid AS bonus_service_all_code
    FROM {{ tf_source('ds_cleverbox__raw__bonus_employee') }}
),

bonus_employee AS (
    SELECT * EXCEPT (speciality)
    FROM {{ tf_ref('ds_cleverbox__bonus_employee') }}
),

discount_usage AS (
    SELECT * EXCEPT (date, specialist, client_name, value)
    FROM {{ tf_ref('ds_cleverbox__discount_usage') }}
),

certificates_and_balance AS (
    SELECT
        ruid AS certificates_id,
        SUM(sum) AS certificates_sum
    FROM {{ tf_source('ds_cleverbox__raw__certificates_and_balance') }}
    GROUP BY ruid
),

intermediate_step_1_source AS (
    SELECT
        *,
        CASE
            WHEN bonus_service_code_code IS NOT NULL THEN bonus_service_code_code
            WHEN bonus_service_category_code IS NOT NULL THEN bonus_service_category_code
            WHEN bonus_service_all_code IS NOT NULL THEN bonus_service_all_code
        END AS bonus_code,
        CONCAT('DISCOUNT++', FORMAT_DATE('%d.%m.%Y', date), '--', name, '--', specialist, '--', client_name) AS druid
    FROM {{ tf_ref('ds_cleverbox__service_sales') }} AS service_sales
    LEFT JOIN employees_speciality
        ON service_sales.specialist = employees_speciality.employees_speciality_name
    LEFT JOIN services
        ON LPAD(CAST(service_sales.code AS STRING), 6, '0') = services.guid
    LEFT JOIN bonus_service_code
        ON CONCAT(service_sales.specialist, '-Послуга-', service_sales.code) = bonus_service_code.bonus_service_code_code
    LEFT JOIN bonus_service_category
        ON CONCAT(service_sales.specialist, '-Послуга-', service_sales.category) = bonus_service_category.bonus_service_category_code
    LEFT JOIN bonus_service_all
        ON CONCAT(service_sales.specialist, '-Послуга-ВСЕ') = bonus_service_all.bonus_service_all_code
),

intermediate_step_2_source AS (
    SELECT
        *,
        CASE
            WHEN subscription > 0 THEN 'АБОНЕМЕНТ'
            WHEN ruid IS NULL THEN ''
            WHEN discount_name IS NULL OR discount_name = '' THEN 'АБОНЕМЕНТ'
            ELSE discount_name
        END AS discount_name_source
    FROM intermediate_step_1_source
    LEFT JOIN discount_usage
        ON intermediate_step_1_source.druid = discount_usage.ruid
),

source AS (
    SELECT *
    FROM intermediate_step_2_source
    LEFT JOIN bonus_employee
        ON intermediate_step_2_source.bonus_code = bonus_employee.uid
    LEFT JOIN {{ tf_ref('ds_cleverbox__bonus_discount') }} AS bonus_discount
        ON intermediate_step_2_source.discount_name_source = bonus_discount.bonus_discount_name
    LEFT JOIN {{ tf_ref('ds_cleverbox__bonus_adjustments') }} AS bonus_adjustment
        ON intermediate_step_2_source.id = bonus_adjustment.ruid
    LEFT JOIN certificates_and_balance
        ON intermediate_step_2_source.id = certificates_and_balance.certificates_id

)

{{ tf_transform_model('source') }}
