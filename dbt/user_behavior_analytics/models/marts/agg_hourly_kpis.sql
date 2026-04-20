WITH hourly_clicks AS (
    SELECT
        DATE_TRUNC('hour', clicked_at)  AS hour_timestamp,
        country,
        device,
        product_category,
        COUNT(*)                        AS total_clicks,
        COUNT(DISTINCT user_id)         AS unique_users,
        COUNT(DISTINCT session_id)      AS unique_sessions
    FROM {{ ref('stg_clicks') }}
    GROUP BY 1, 2, 3, 4
),
hourly_revenue AS (
    SELECT
        DATE_TRUNC('hour', purchased_at) AS hour_timestamp,
        country,
        product_category,
        COUNT(DISTINCT order_id)         AS total_orders,
        SUM(total_amount_usd)            AS total_revenue_usd
    FROM {{ ref('stg_purchases') }}
    GROUP BY 1, 2, 3
)
SELECT
    c.hour_timestamp,
    c.country,
    c.device,
    c.product_category,
    c.total_clicks,
    c.unique_users,
    c.unique_sessions,
    COALESCE(r.total_orders, 0)       AS total_orders,
    COALESCE(r.total_revenue_usd, 0)  AS total_revenue_usd,
    ROUND(
        COALESCE(r.total_orders, 0) * 100.0 / NULLIF(c.unique_sessions, 0),
    2)                                AS conversion_rate_pct,
    CURRENT_TIMESTAMP()               AS dbt_updated_at
FROM hourly_clicks c
LEFT JOIN hourly_revenue r
    ON  c.hour_timestamp    = r.hour_timestamp
    AND c.country           = r.country
    AND c.product_category  = r.product_category