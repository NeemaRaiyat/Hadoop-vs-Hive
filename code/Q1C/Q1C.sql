SELECT concat('ss_sold_date_sk_',ss_sold_date_sk), SUM(ss_net_paid_inc_tax) as taxSum
FROM store_sales
WHERE ss_sold_date_sk >= 2450815 AND ss_sold_date_sk <= 2452815
GROUP BY (ss_sold_date_sk)
ORDER BY taxSum DESC
LIMIT 10;

-- explanation:
-- The query groups each date by ss_sold_date_sk and sums all of the net paid including tax
-- (ss_net_paid_inc_tax) for each day in the given time period. This is then ordered in descending
-- order and limited by k.