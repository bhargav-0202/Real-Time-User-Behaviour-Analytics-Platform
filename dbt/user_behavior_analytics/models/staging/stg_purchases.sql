WITH source AS (
    SELECT * FROM {{ source('raw_events', 'RAW_PURCHASES') }}
)
SELECT
    EVENT_ID            AS purchase_id,
    ORDER_ID            AS order_id,
    USER_ID             AS user_id,
    SESSION_ID          AS session_id,
    PRODUCT_ID          AS product_id,
    PRODUCT_NAME        AS product_name,
    PRODUCT_CATEGORY    AS product_category,
    QUANTITY            AS quantity,
    UNIT_PRICE          AS unit_price_usd,
    TOTAL_AMOUNT        AS total_amount_usd,
    PAYMENT_METHOD      AS payment_method,
    COUNTRY             AS country,
    IS_FRAUDULENT       AS is_fraudulent,
    EVENT_TIMESTAMP     AS purchased_at,
    DATE(EVENT_TIMESTAMP) AS purchase_date,
    CURRENT_TIMESTAMP() AS ingested_at
FROM source
WHERE EVENT_ID IS NOT NULL AND TOTAL_AMOUNT > 0