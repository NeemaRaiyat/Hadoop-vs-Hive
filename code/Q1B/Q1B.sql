SELECT concat('ss_store_sk_',ranks.s1), concat('ss_item_sk_',ranks.s2), ranks.s3 FROM 

(SELECT myStore.ss_store_sk AS s1, mySales.ss_item_sk AS s2, mySales.itemTotal AS s3, myStore.storeTotal AS s4, 
row_number() over (PARTITION BY myStore.ss_store_sk ORDER BY mySales.itemTotal ASC) as rank
FROM
(SELECT ss_store_sk, SUM(ss_quantity) AS storeTotal 
FROM store_sales
WHERE ss_sold_date_sk >= 2450815 AND ss_sold_date_sk <= 2452315
GROUP BY ss_store_sk ORDER BY storeTotal DESC LIMIT 5) AS myStore

JOIN

(SELECT ss_store_sk, ss_item_sk, SUM(ss_quantity) AS itemTotal 
FROM store_sales
WHERE ss_sold_date_sk >= 2450815 AND ss_sold_date_sk <= 2452315
GROUP BY ss_store_sk, ss_item_sk) AS mySales

ON myStore.ss_store_sk == mySales.ss_store_sk
) AS ranks

WHERE rank <= 3
ORDER BY ranks.s4 DESC, ranks.rank ASC;

-- explanation:
-- The top N stores are found using the sum of the sale quantities for each item of each store.
-- This is joined to another query to find all the quantities of items sold by these top N stores.
-- Using the row_number() function the records are ranked so that the items with the least number
-- of items sold have the smallest rank for each store. The rank is then limited by M so only the M 
-- fewest items sold are selected. 