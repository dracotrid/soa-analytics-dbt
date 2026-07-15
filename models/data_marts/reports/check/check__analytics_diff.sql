WITH LT AS (
    SELECT
        CONCAT(product_type, '_', period) AS eid,
        product_type,
        period,
        ROUND(CAST(income AS NUMERIC), 2) AS income,
        ROUND(CAST(profit AS NUMERIC), 2) AS profit,
        ROUND(CAST(bonus AS NUMERIC), 2) AS bonus
    FROM {{ tf_ref('check__analytics_prod') }}
),

RT AS (
    SELECT
        CONCAT(product_type, '_', period) AS eid,
        product_type,
        period,
        ROUND(CAST(income AS NUMERIC), 2) AS income,
        ROUND(CAST(profit AS NUMERIC), 2) AS profit,
        ROUND(CAST(bonus AS NUMERIC), 2) AS bonus
    FROM {{ tf_ref('check__analytics_actual') }}
),

LTV AS (
    SELECT
        eid AS id,
        CAST(TO_JSON_STRING((SELECT AS STRUCT LT.* EXCEPT (eid))) AS STRING) AS fvalues
    FROM LT
),

RTV AS (
    SELECT
        eid AS id,
        CAST(TO_JSON_STRING((SELECT AS STRUCT RT.* EXCEPT (eid))) AS STRING) AS fvalues
    FROM RT
),

JOINED AS (
    SELECT
        COALESCE(LTV.id, RTV.id) AS id,
        LTV.fvalues AS lt_fvalues,
        RTV.fvalues AS rt_fvalues,
        SAFE.PARSE_JSON(LTV.fvalues) AS lt_json,
        SAFE.PARSE_JSON(RTV.fvalues) AS rt_json
    FROM LTV
    FULL OUTER JOIN RTV USING (id)
),

DIFFED AS (
    SELECT
        id,
        lt_fvalues,
        rt_fvalues,
        JSON_OBJECT(
            ARRAY_AGG(k ORDER BY k),
            ARRAY_AGG(
                JSON_OBJECT(
                    'LT',
                    IF(k IN UNNEST(COALESCE(JSON_KEYS(lt_json), [])), lt_json[k], TO_JSON('MISSED')),
                    'RT',
                    IF(k IN UNNEST(COALESCE(JSON_KEYS(rt_json), [])), rt_json[k], TO_JSON('MISSED'))
                )
                ORDER BY k
            )
        ) AS diff
    FROM JOINED,
        UNNEST(
            ARRAY(
                SELECT DISTINCT k
                FROM UNNEST(ARRAY_CONCAT(COALESCE(JSON_KEYS(lt_json), []), COALESCE(JSON_KEYS(rt_json), []))) AS k
                WHERE k != 'id'
            )
        ) AS k
    WHERE
        k NOT IN UNNEST(COALESCE(JSON_KEYS(lt_json), []))
        OR k NOT IN UNNEST(COALESCE(JSON_KEYS(rt_json), []))
        OR TO_JSON_STRING(lt_json[k]) IS DISTINCT FROM TO_JSON_STRING(rt_json[k])
    GROUP BY id, lt_fvalues, rt_fvalues
),

FINAL AS (
    SELECT
        id,
        lt_fvalues AS prod_value,
        rt_fvalues AS actual_value,
        TO_JSON_STRING(diff) AS difference
    FROM DIFFED
    ORDER BY id
)

{{ tf_transform_model('final') }}
