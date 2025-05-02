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
        direction AS service_direction
    FROM {{ tf_ref('ds_cleverbox__services') }}
),

source AS (
    SELECT *
    FROM {{ tf_ref('ds_cleverbox__service_sales') }} AS service_sales
    LEFT JOIN employees_speciality
        ON service_sales.specialist = employees_speciality.employees_speciality_name
    LEFT JOIN services
        ON service_sales.code = services.guid
)

{{ tf_transform_model('source') }}
