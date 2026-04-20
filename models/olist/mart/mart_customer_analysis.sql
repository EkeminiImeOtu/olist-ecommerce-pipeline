{{ config(schema='OLIST_MART', tags=['mart']) }}

SELECT
    c.CUSTOMER_STATE,
    c.CUSTOMER_CITY,
    COUNT(DISTINCT c.CUSTOMER_UNIQUE_ID)    AS TOTAL_CUSTOMERS,
    COUNT(DISTINCT o.ORDER_ID)              AS TOTAL_ORDERS,
    SUM(i.PRICE + i.FREIGHT_VALUE)          AS TOTAL_SPENT,
    ROUND(AVG(i.PRICE + i.FREIGHT_VALUE),2) AS AVG_ORDER_VALUE,
    ROUND(AVG(r.REVIEW_SCORE), 2)           AS AVG_SATISFACTION_SCORE
FROM {{ ref('stg_customers') }} c
JOIN {{ ref('stg_orders') }} o      ON c.CUSTOMER_ID = o.CUSTOMER_ID
JOIN {{ ref('stg_order_items') }} i ON o.ORDER_ID = i.ORDER_ID
LEFT JOIN {{ ref('stg_reviews') }} r ON o.ORDER_ID = r.ORDER_ID
GROUP BY 1, 2
ORDER BY TOTAL_SPENT DESC
