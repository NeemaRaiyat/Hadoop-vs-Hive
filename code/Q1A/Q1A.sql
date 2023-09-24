SELECT concat('ss_store_sk_',ss_store_sk), SUM(ss_net_paid) AS net 
FROM store_sales 
WHERE ss_sold_date_sk >= 2450815 
    AND ss_sold_date_sk <= 2452815
    AND ss_store_sk IS NOT NULL 
GROUP BY ss_store_sk 
ORDER BY net ASC 
LIMIT 10;

-- explanation:
-- The query groups each unique store by the ss_store_sk and sums all of the revenue for each item sold by that store
-- in the given time period. THis is then ordered in ascending order and limited by k.