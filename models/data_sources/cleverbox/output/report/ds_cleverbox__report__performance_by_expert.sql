WITH
src__report_services AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__report_services') }}
),

src__report_goods AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__report_goods') }}
),

src__employees AS (
    SELECT * FROM {{ tf_ref('ds_cleverbox__prepared__employees') }}
),

input_service AS (
    SELECT
        specialist,
        client_name,
        date,
        year,
        month,
        day,
        income_total,
        profit_total,
        bonus_total,
        -- CONCAT(year, "-", IF(month<10, "0"||CAST(month AS STRING), CAST(month AS STRING)), "-", IF(day<10, "0"||CAST(day AS STRING), CAST(day AS STRING))) period, --noqa:LT05
        CONCAT(YEAR, "-", IF(month < 10, "0" || CAST(month AS STRING), CAST(month AS STRING)), "-01") AS period,
        * EXCEPT (specialist, client_name, date, year, month, day, income_total, profit_total, bonus_total)
    FROM src__report_services
    WHERE income_total > 0
),

input_goods AS (
    SELECT
        specialist,
        client_name,
        date,
        year,
        month,
        day,
        income_total,
        profit_total,
        bonus_total,
        -- CONCAT(year, "-", IF(month<10, "0"||CAST(month AS STRING), CAST(month AS STRING)), "-", IF(day<10, "0"||CAST(day AS STRING), CAST(day AS STRING))) period, --noqa:LT05
        CONCAT(YEAR, "-", IF(month < 10, "0" || CAST(month AS STRING), CAST(month AS STRING)), "-01") AS period,
        * EXCEPT (specialist, client_name, date, year, month, day, income_total, profit_total, bonus_total)
    FROM src__report_goods
    WHERE income_total > 0
),

prepared_service AS (
    SELECT
        "SERVICE" AS ptype,
        specialist,
        client_name,
        period,
        year,
        month,
        day,
        1 AS visit_count,
        COUNT(*) AS item_count,
        SUM(income_total) AS income_total,
        SUM(profit_total) AS profit_total,
        SUM(bonus_total) AS bonus_total
    FROM input_service
    GROUP BY specialist, client_name, period, year, month, day
    ORDER BY specialist, year, month, day, client_name
),

prepared_goods AS (
    SELECT
        "GOODS" AS ptype,
        specialist,
        client_name,
        period,
        year,
        month,
        day,
        1 AS visit_count,
        COUNT(*) AS item_count,
        SUM(income_total) AS income_total,
        SUM(profit_total) AS profit_total,
        SUM(bonus_total) AS bonus_total
    FROM input_goods
    GROUP BY specialist, client_name, period, year, month, day
    ORDER BY specialist, year, month, day, client_name
),

prepared_product AS (
    SELECT
        t.*,
        job_title
    FROM (
        SELECT * FROM prepared_service
        UNION ALL
        SELECT * FROM prepared_goods
    ) AS t
    LEFT JOIN src__employees AS empl ON t.specialist = empl.name
    WHERE t.year = 2025
    -- and t.month = 5
),

day_count AS (
    SELECT
        specialist,
        period,
        COUNT(DISTINCT DAY) AS day_count
    FROM
        prepared_service
    GROUP BY
        specialist,
        period
),

kpi_service AS (
    SELECT DISTINCT
        specialist,
        period,
        job_title,
        CONCAT(specialist, period, job_title) AS ruid,
        SUM(visit_count) OVER (PARTITION BY specialist, period) AS visit_count_total,

        SUM(item_count) OVER (PARTITION BY specialist, period) AS service_count_total,
        ROUND(AVG(item_count) OVER (PARTITION BY specialist, period), 2) AS service_count_avg,
        PERCENTILE_CONT(item_count, 0.5) OVER (PARTITION BY specialist, period) AS service_count_50p,

        SUM(income_total) OVER (PARTITION BY specialist, period) AS service_income_total,
        ROUND(AVG(income_total) OVER (PARTITION BY specialist, period), 2) AS visit_service_income_avg,
        PERCENTILE_CONT(income_total, 0.5) OVER (PARTITION BY specialist, period) AS visit_service_income_50p,

        SUM(profit_total) OVER (PARTITION BY specialist, period) AS service_profit_total,
        ROUND(AVG(profit_total) OVER (PARTITION BY specialist, period), 2) AS visit_service_profit_avg,
        PERCENTILE_CONT(profit_total, 0.5) OVER (PARTITION BY specialist, period) AS visit_service_profit_50p,

        SUM(bonus_total) OVER (PARTITION BY specialist, period) AS service_bonus_total
    FROM prepared_product
    WHERE ptype = "SERVICE"
--ORDER BY period, job_title, specialist
),

kpi_goods AS (
    SELECT DISTINCT
        specialist,
        period,
        job_title,
        CONCAT(specialist, period, job_title) AS ruid,
        SUM(visit_count) OVER (PARTITION BY specialist, period) AS visit_count_total,

        SUM(item_count) OVER (PARTITION BY specialist, period) AS goods_count_total,
        ROUND(AVG(item_count) OVER (PARTITION BY specialist, period), 2) AS goods_count_avg,
        PERCENTILE_CONT(item_count, 0.5) OVER (PARTITION BY specialist, period) AS goods_count_50p,

        SUM(income_total) OVER (PARTITION BY specialist, period) AS goods_income_total,
        ROUND(AVG(income_total) OVER (PARTITION BY specialist, period), 2) AS visit_goods_income_avg,
        PERCENTILE_CONT(income_total, 0.5) OVER (PARTITION BY specialist, period) AS visit_goods_income_50p,

        SUM(profit_total) OVER (PARTITION BY specialist, period) AS goods_profit_total,
        ROUND(AVG(profit_total) OVER (PARTITION BY specialist, period), 2) AS visit_goods_profit_avg,
        PERCENTILE_CONT(profit_total, 0.5) OVER (PARTITION BY specialist, period) AS visit_goods_profit_50p,

        SUM(bonus_total) OVER (PARTITION BY specialist, period) AS goods_bonus_total
    FROM prepared_product
    WHERE ptype = "GOODS"
--ORDER BY period, job_title, specialist
),

kpi_all AS (
    SELECT DISTINCT
        specialist,
        period,
        job_title,
        CONCAT(specialist, period, job_title) AS ruid,
        SUM(visit_count) OVER (PARTITION BY specialist, period) AS visit_count_total,

        SUM(item_count) OVER (PARTITION BY specialist, period) AS product_count_total,
        ROUND(AVG(item_count) OVER (PARTITION BY specialist, period), 2) AS product_count_avg,
        PERCENTILE_CONT(item_count, 0.5) OVER (PARTITION BY specialist, period) AS product_count_50p,

        SUM(income_total) OVER (PARTITION BY specialist, period) AS product_income_total,
        ROUND(AVG(income_total) OVER (PARTITION BY specialist, period), 2) AS visit_product_income_avg,
        PERCENTILE_CONT(income_total, 0.5) OVER (PARTITION BY specialist, period) AS visit_product_income_50p,

        SUM(profit_total) OVER (PARTITION BY specialist, period) AS product_profit_total,
        ROUND(AVG(profit_total) OVER (PARTITION BY specialist, period), 2) AS visit_product_profit_avg,
        PERCENTILE_CONT(profit_total, 0.5) OVER (PARTITION BY specialist, period) AS visit_product_profit_50p,

        SUM(bonus_total) OVER (PARTITION BY specialist, period) AS product_bonus_total
    FROM prepared_product
--ORDER BY period, job_title, specialist
),

kpi_detailed AS (
    SELECT
        t.ruid,
        t.specialist,
        t.period,
        t.job_title,
        COALESCE(s.visit_count_total, 0) AS visit_count,
        COALESCE(g.visit_count_total, 0) AS visit_goods_count,
        s.* EXCEPT (ruid, specialist, period, job_title, visit_count_total),
        g.* EXCEPT (ruid, specialist, period, job_title, visit_count_total),
        a.* EXCEPT (ruid, specialist, period, job_title, visit_count_total)
    FROM (
        SELECT DISTINCT
            ruid,
            specialist,
            period,
            job_title
        FROM kpi_all
    ) AS t
    LEFT JOIN kpi_all AS a ON t.ruid = a.ruid
    LEFT JOIN kpi_service AS s ON t.ruid = s.ruid
    LEFT JOIN kpi_goods AS g ON t.ruid = g.ruid
),


final_kpi AS (
    SELECT
        kpi.period,
        kpi.specialist,
        job_title,
        COALESCE(day_count, 0) AS day_count,
        visit_count,
        visit_goods_count,
        ROUND(IF(visit_count > 0 AND visit_count >= visit_goods_count, visit_goods_count / visit_count, 0), 4) AS visit_goods_rate,


        COALESCE(service_count_total, 0) AS service_count_total,
        COALESCE(goods_count_total, 0) AS goods_count_total,
        COALESCE(product_count_total, 0) AS product_count_total,

        COALESCE(service_count_avg, 0) AS service_count_avg,
        COALESCE(goods_count_avg, 0) AS goods_count_avg,
        COALESCE(product_count_avg, 0) AS product_count_avg,

        -- COALESCE(service_count_50p, 0) AS service_count_50p,
        -- COALESCE(goods_count_50p, 0) AS goods_count_50p,
        -- COALESCE(product_count_50p, 0) AS product_count_50p,

        COALESCE(service_income_total, 0) AS service_income_total,
        COALESCE(goods_income_total, 0) AS goods_income_total,
        COALESCE(product_income_total, 0) AS product_income_total,

        COALESCE(service_bonus_total, 0) AS service_bonus_total,
        COALESCE(goods_bonus_total, 0) AS goods_bonus_total,
        COALESCE(product_bonus_total, 0) AS product_bonus_total,

        COALESCE(service_profit_total, 0) AS service_profit_total,
        COALESCE(goods_profit_total, 0) AS goods_profit_total,
        COALESCE(product_profit_total, 0) AS product_profit_total,

        COALESCE(visit_service_income_50p, 0) AS visit_service_income_avg,
        COALESCE(visit_goods_income_50p, 0) AS visit_goods_income_avg,
        COALESCE(visit_product_income_50p, 0) AS visit_product_income_avg,

        COALESCE(visit_service_profit_50p, 0) AS visit_service_profit_avg,
        COALESCE(visit_goods_profit_50p, 0) AS visit_goods_profit_avg,
        COALESCE(visit_product_profit_50p, 0) AS visit_product_profit_avg,

        ROUND(service_profit_total / service_income_total, 4) AS service_margin,
        ROUND(goods_profit_total / goods_income_total, 4) AS goods_margin,
        ROUND(product_profit_total / product_income_total, 4) AS product_margin
    FROM kpi_detailed AS kpi
    LEFT JOIN day_count AS dcntv ON kpi.specialist = dcntv.specialist AND kpi.period = dcntv.period
)

SELECT * FROM final_kpi
ORDER BY period, job_title, specialist
