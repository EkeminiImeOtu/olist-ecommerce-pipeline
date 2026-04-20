{{ config(schema='OLIST_MART', tags=['mart']) }}

SELECT
    DATE_TRUNC('month', o.ORDER_PURCHASE_TIMESTAMP) AS ORDER_MONTH,
    COUNT(DISTINCT o.ORDER_ID)                       AS TOTAL_ORDERS,
    COUNT(DISTINCT o.CUSTOMER_ID)                    AS UNIQUE_CUSTOMERS,
    SUM(i.PRICE)                                     AS TOTAL_REVENUE,
    SUM(i.FREIGHT_VALUE)                             AS TOTAL_FREIGHT,
    SUM(i.PRICE + i.FREIGHT_VALUE)                   AS TOTAL_GMV,
    ROUND(AVG(i.PRICE + i.FREIGHT_VALUE), 2)         AS AVG_ORDER_VALUE,
    COUNT(DISTINCT CASE WHEN o.ORDER_STATUS = 'delivered' THEN o.ORDER_ID END) AS DELIVERED_ORDERS,
    COUNT(DISTINCT CASE WHEN o.ORDER_STATUS = 'cancelled' THEN o.ORDER_ID END) AS CANCELLED_ORDERS
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} i ON o.ORDER_ID = i.ORDER_ID
GROUP BY 1
ORDER BY 1
