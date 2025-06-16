WITH employees_speciality AS (
    SELECT
        name AS employees_speciality_name,
        job_title AS speciality
    FROM {{ tf_ref('ds_cleverbox__processed__employees') }}
    GROUP BY name, job_title
),

services AS (
    SELECT
        guid,
        category AS service_category,
        direction AS service_direction,
        cost_price AS service_cost_price
    FROM {{ tf_ref('ds_cleverbox__processed__services') }}
),

final AS (
    SELECT *
    FROM {{ tf_ref('ds_cleverbox__processed__services_sales') }} AS service_sales
    LEFT JOIN employees_speciality
        ON service_sales.specialist = employees_speciality.employees_speciality_name
    LEFT JOIN services
        ON LPAD(CAST(service_sales.code AS STRING), 6, '0') = services.guid
)

{{ tf_transform_model('final') }}
