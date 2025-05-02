WITH employees_speciality AS (
    SELECT
        name AS employees_speciality_name,
        job_title AS speciality
    FROM {{ tf_ref('ds_cleverbox__employees') }}
    GROUP BY name, job_title
),

source AS (
    SELECT *
    FROM {{ tf_ref('ds_cleverbox__service_sales') }} AS service_sales
    LEFT JOIN employees_speciality
        ON service_sales.specialist = employees_speciality.employees_speciality_name
)
{{ tf_transform_model('source') }}
