{{ config(schema='OLIST_MART', tags=['mart']) }}

SELECT
    p.PRODUCT_CATEGORY,
    p.PRODUCT_ID,
    COUNT(DISTINCT i.ORDER_ID)      AS TOTAL_ORDERS,
    SUM(i.ORDER_ITEM_ID)            AS TOTAL_UNITS_SOLD,
    SUM(i.PRICE)                    AS TOTAL_REVENUE,
    ROUND(AVG(i.PRICE), 2)          AS AVG_SELLING_PRICE,
    ROUND(AVG(r.REVIEW_SCORE), 2)   AS AVG_REVIEW_SCORE,
    COUNT(r.REVIEW_ID)              AS TOTAL_REVIEWS
FROM {{ ref('stg_order_items') }} i
JOIN {{ ref('stg_products') }} p     ON i.PRODUCT_ID = p.PRODUCT_ID
LEFT JOIN {{ ref('stg_orders') }} o  ON i.ORDER_ID = o.ORDER_ID
LEFT JOIN {{ ref('stg_reviews') }} r ON o.ORDER_ID = r.ORDER_ID
GROUP BY 1, 2
ORDER BY TOTAL_REVENUE DESC
