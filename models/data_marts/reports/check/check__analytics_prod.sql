WITH prepared_data AS (
    SELECT
        product_type,
        income,
        profit,
        bonus,
        EXTRACT(YEAR FROM date) AS sales_year,
        FORMAT_DATE('%Y-%m', date) AS sales_month
    FROM `solomia-analytics-main.soa_dev_smm_dm_reports.prepared__product_sales`
),

final AS (
    SELECT
        product_type,
        CASE
            WHEN GROUPING(sales_year) = 1 AND GROUPING(sales_month) = 1 THEN 'All'
            WHEN GROUPING(sales_month) = 1 THEN CAST(sales_year AS STRING)
            ELSE sales_month
        END AS period,
        SUM(income) AS income,
        SUM(profit) AS profit,
        SUM(bonus) AS bonus
    FROM prepared_data
    GROUP BY
        GROUPING SETS (
            (product_type),
            (product_type, sales_year),
            (product_type, sales_month)
        )
    ORDER BY product_type, period
)

{{ tf_transform_model('final') }}
