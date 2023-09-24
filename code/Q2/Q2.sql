SELECT ss_store_sk, s_floor_space, sum(ss_net_paid) as net 
FROM store_sales
JOIN store ON (ss_store_sk == s_store_sk)
WHERE ss_sold_date_sk >= 2451146 AND ss_sold_date_sk <= 2452268 AND ss_store_sk IS NOT NULL
GROUP BY ss_store_sk, s_floor_space
ORDER BY net DESC, s_floor_space DESC
LIMIT 10; 

-- explanation:
-- The query groups each unique store by the ss_store_sk and sums all of the revenue for each item sold by that store
-- in the given time period. THis is then ordered in ascending order and limited by k.

-- The query sums the net paid of items for each store within the date range from store_sales.
-- This is is then joined with store to get the floor space associated with each store
-- We then limit the results by k
