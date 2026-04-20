WITH source AS (
    SELECT * FROM {{ source('raw_events', 'RAW_CLICKS') }}
)
SELECT
    EVENT_ID            AS click_id,
    USER_ID             AS user_id,
    SESSION_ID          AS session_id,
    PRODUCT_ID          AS product_id,
    PRODUCT_NAME        AS product_name,
    PRODUCT_CATEGORY    AS product_category,
    PAGE                AS page,
    DEVICE              AS device,
    COUNTRY             AS country,
    REFERRER            AS referrer,
    PRICE_SEEN          AS price_seen_usd,
    EVENT_TIMESTAMP     AS clicked_at,
    DATE(EVENT_TIMESTAMP) AS click_date,
    CURRENT_TIMESTAMP() AS ingested_at
FROM source
WHERE EVENT_ID IS NOT NULL