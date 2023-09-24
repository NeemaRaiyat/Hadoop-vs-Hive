# Hadoop vs Hive
### Neema Raiyat

---

### Directory Structure

```markdown
├── code
│   ├── Q1A
│   │   ├── Q1A.java
│   │   └── Q1A.sql
│   ├── Q1B
│   │   ├── Q1B.java
│   │   └── Q1B.sql
│   ├── Q1C
│   │   ├── Q1C.java
│   │   └── Q1C.sql
│   └── Q2
│       ├── Q2Memory.java
│       ├── Q2Reduced.java
│       └── Q2.sql
└────── schema.sql
```

---

### Initial Setup

Run the following commands within the terminal:

```bash
source cs346env.sh
$HADOOP_HOME/sbin/start-all.sh
```

Run the command below to check all nodes have been successfully created:

```bash
jps
```
Expected output:

```bash
NameNode
ResourceManager
DataNode
Jps
NodeManager
SecondaryNameNode
```
---

## Running `Hadoop MapReduce` Queries

### Question 1A - Bottom-K Query

Compile the code with the following commands: \
*(Please note the code has already been compiled, you can skip to the example command.)*

```bash
hadoop com.sun.tools.javac.Main Q1A.java
jar cf Q1A.jar Q1A*.class
```

Example command:\
(K = 10, Start Date = 2451146, End Date = 2452268)
```bash
hadoop jar Q1A.jar Q1A 10 2451146 2452268 input/40G/store_sales output/Q1A00
```

View output with this command:
```bash
hdfs dfs -cat output/Q1A00/part-r-00000
```

Example output of above command:
```bash
ss_store_sk_109 2000891451.65
ss_store_sk_62  2001356972.77
ss_store_sk_73  2002496753.42
ss_store_sk_13  2005044284.81
ss_store_sk_26  2006646910.95
ss_store_sk_37  2007411333.56
ss_store_sk_56  2007617878.05
ss_store_sk_110 2007840718.64
ss_store_sk_76  2009872832.22
ss_store_sk_79  2010489119.70
```

### Question 1B - Which items sold least in period?

Compile the code with the following commands:\
*(Please note the code has already been compiled, you can skip to the example command.)*

```bash
hadoop com.sun.tools.javac.Main Q1B.java
jar cf Q1B.jar Q1B*.class
```

Example command:\
(M = 3, N = 2, Start Date = 2451146, End Date = 2452268)
```bash
hadoop jar Q1B.jar Q1B 3 2 2451146 2452268 input/40G/store_sales output/Q1B00
```

View output with this command:
```bash
hdfs dfs -cat output/Q1B00/part-r-00000
```

Example output of above command:
```bash
ss_store_sk_92  ss_item_sk_3309    46
ss_store_sk_92  ss_item_sk_10233    80
ss_store_sk_92  ss_item_sk_7587    87
ss_store_sk_50  ss_item_sk_48207    62
ss_store_sk_50  ss_item_sk_18105    133
ss_store_sk_50  ss_item_sk_21729    139
```

### Question 1C - Which days among range have highest total value of ss_net_paid_inc_tax?

Compile the code with the following commands:\
*(Please note the code has already been compiled, you can skip to the example command.)*

```bash
hadoop com.sun.tools.javac.Main Q1C.java
jar cf Q1C.jar Q1C*.class
```

Example command:\
(K = 10, Start Date = 2451146, End Date = 2452268)
```bash
hadoop jar Q1C.jar Q1C 10 2451392 2451894 input/40G/store_sales output/Q1C00
```

View output with this command:
```bash
hdfs dfs -cat output/Q1C00/part-r-00000
```

Example output of above command:
```bash
ss_sold_date_sk_2451546 269011207.69
ss_sold_date_sk_2451522 218299535.32
ss_sold_date_sk_2451544 216896013.28
ss_sold_date_sk_2451537 215618328.78
ss_sold_date_sk_2451521 215392369.46
ss_sold_date_sk_2451533 214772795.73
ss_sold_date_sk_2451532 214618693.96
ss_sold_date_sk_2451851 213953384.07
ss_sold_date_sk_2451891 213918317.29
ss_sold_date_sk_2451880 213849908.61

```

### Question 2 - Joins

Compile the code with the following commands:\
*(Please note the code has already been compiled, you can skip to the example commands.)*

> Memory-Backed Join
```bash
hadoop com.sun.tools.javac.Main Q2Memory.java
jar cf Q2Memory.jar Q2Memory*.class
```

> Reduce-Side Join
```bash
hadoop com.sun.tools.javac.Main Q2Reduced.java
jar cf Q2Reduced.jar Q2Reduced*.class
```

Example command:\
(K = 10, Start Date = 2451146, End Date = 2452268)

> Memory-Backed Join
```bash
hadoop jar Q2Memory.jar Q2Memory 10 2451146 2452268 input/40G/store_sales/store_sales.dat input/40G/store/store.dat output/Q2Memory00
```

> Reduce-Side Join
```bash
hadoop jar Q2Reduced.jar Q2Reduced 10 2451146 2452268 input/40G/store_sales/store_sales.dat input/40G/store/store.dat output/Q2Reduced00
```

View output with this command:

> Memory-Backed Join
```bash
hdfs dfs -cat output/Q2Memory00/part-r-00000
```

> Reduce-Side Join
```bash
hdfs dfs -cat output/Q2Reduced00/part-r-00000
```

Example output of above commands:\
*(Note: order of ss_net_paid and s_floor_space columns was not specified.)*
> Memory-Backed Join
```bash
92      8573853 2039983202.07
50      7825489 2036624698.89
8       6995995 2032733764.65
32      6131757 2028710419.82
4       9341467 2028145948.34
94      9599785 2026137085.11
38      5813235 2024183575.31
82      6035829 2023916557.05
97      7232715 2023659816.02
112     6081074 2023012491.83
```

> Reduce-Side Join
```bash
92      2039983202.07   8573853
50      2036624698.89   7825489
8       2032733764.65   6995995
32      2028710419.82   6131757
4       2028145948.34   9341467
94      2026137085.11   9599785
38      2024183575.31   5813235
82      2023916557.05   6035829
97      2023659816.02   7232715
112     2023012491.83   6081074
```

---

## Running `HiveQL` Queries

### Setup

Copy and run the schema provided below: \
*(This code is taken directly from the schema.sql file.)*

```sql
DROP TABLE store;
CREATE EXTERNAL TABLE store (
s_store_sk int,
s_store_id char(16),
s_rec_start_date date,
s_rec_end_date date,
s_closed_date_sk int,
s_store_name varchar(50),
s_number_employees int,
s_floor_space int,
s_hours char(20),
S_manager varchar(40),
S_market_id int,
S_geography_class varchar(100),
S_market_desc varchar(100),
s_market_manager varchar(40),
s_division_id int,
s_division_name varchar(50),
s_company_id int,
s_company_name varchar(50),
s_street_number varchar(10),
s_street_name varchar(60),
s_street_type char(15),
s_suite_number char(10),
s_city varchar(60),
s_county varchar(30),
s_state char(2),
s_zip char(10),
s_country varchar(20),
s_gmt_offset decimal(5,2),
s_tax_percentage decimal(5,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION 'input/40G/store';

DROP TABLE store_sales;
CREATE EXTERNAL TABLE store_sales (
ss_sold_date_sk int,
ss_sold_time_sk int,
ss_item_sk int,
ss_customer_sk int,
ss_cdemo_sk int,
ss_hdemo_sk int,
ss_addr_sk int,
ss_store_sk int,
ss_promo_sk int,
ss_ticket_number int,
ss_quantity int,
ss_wholesale_cost decimal(7,2),
ss_list_price decimal(7,2),
ss_sales_price decimal(7,2),
ss_ext_discount_amt decimal(7,2),
ss_ext_sales_price decimal(7,2),
ss_ext_wholesale_cost decimal(7,2),
ss_ext_list_price decimal(7,2),
ss_ext_tax decimal(7,2),
ss_coupon_amt decimal(7,2),
ss_net_paid decimal(7,2),
ss_net_paid_inc_tax decimal(7,2),
ss_net_profit decimal(7,2) 
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION 'input/40G/store_sales';
```

### Question 1A - Bottom-K Query

Copy and run this `hive` query: \
(K = 10, Start Date = 2451146, End Date = 2452268)

```sql
SELECT concat('ss_store_sk_',ss_store_sk), SUM(ss_net_paid) AS net 
FROM store_sales 
WHERE ss_sold_date_sk >= 2451146 
    AND ss_sold_date_sk <= 2452268
    AND ss_store_sk IS NOT NULL 
GROUP BY ss_store_sk 
ORDER BY net ASC 
LIMIT 10;
```

Expected output:
```bash
ss_store_sk_109 2000891451.65
ss_store_sk_62  2001356972.77
ss_store_sk_73  2002496753.42
ss_store_sk_13  2005044284.81
ss_store_sk_26  2006646910.95
ss_store_sk_37  2007411333.56
ss_store_sk_56  2007617878.05
ss_store_sk_110 2007840718.64
ss_store_sk_76  2009872832.22
ss_store_sk_79  2010489119.70
```

General query: \
*(Enter values for K, START_DATE and END_DATE within the query below.)*
```sql
SELECT concat('ss_store_sk_',ss_store_sk), SUM(ss_net_paid) AS net 
FROM store_sales 
WHERE ss_sold_date_sk >= START_DATE
    AND ss_sold_date_sk <= END_DATE
    AND ss_store_sk IS NOT NULL 
GROUP BY ss_store_sk 
ORDER BY net ASC 
LIMIT K;
```

### Question 1B - Which items sold least in period?

Copy and run this `hive` query: \
(M = 3, N = 2, Start Date = 2451146, End Date = 2452268)
```sql
SELECT concat('ss_store_sk_',ranks.s1), concat('ss_item_sk_',ranks.s2), ranks.s3 FROM 

(SELECT myStore.ss_store_sk AS s1, mySales.ss_item_sk AS s2, mySales.itemTotal AS s3, myStore.storeTotal AS s4, 
row_number() over (PARTITION BY myStore.ss_store_sk ORDER BY mySales.itemTotal ASC) as rank
FROM
(SELECT ss_store_sk, SUM(ss_quantity) AS storeTotal 
FROM store_sales
WHERE ss_sold_date_sk >= 2451146 AND ss_sold_date_sk <= 2452268
GROUP BY ss_store_sk ORDER BY storeTotal DESC LIMIT 2) AS myStore

JOIN

(SELECT ss_store_sk, ss_item_sk, SUM(ss_quantity) AS itemTotal 
FROM store_sales
WHERE ss_sold_date_sk >= 2451146 AND ss_sold_date_sk <= 2452268
GROUP BY ss_store_sk, ss_item_sk) AS mySales

ON myStore.ss_store_sk == mySales.ss_store_sk
) AS ranks

WHERE rank <= 3
ORDER BY ranks.s4 DESC, ranks.rank ASC;
```

Expected output:
```bash
ss_store_sk_92  ss_item_sk_3309         46
ss_store_sk_92  ss_item_sk_10233        80
ss_store_sk_92  ss_item_sk_7587         87
ss_store_sk_50  ss_item_sk_48207        62
ss_store_sk_50  ss_item_sk_18105        133
ss_store_sk_50  ss_item_sk_21729        139
```

General query: \
*(Enter values for M, N, START_DATE and END_DATE within the query below.)*
```sql
SELECT concat('ss_store_sk_',ranks.s1), concat('ss_item_sk_',ranks.s2), ranks.s3 FROM 

(SELECT myStore.ss_store_sk AS s1, mySales.ss_item_sk AS s2, mySales.itemTotal AS s3, myStore.storeTotal AS s4, 
row_number() over (PARTITION BY myStore.ss_store_sk ORDER BY mySales.itemTotal ASC) as rank
FROM
(SELECT ss_store_sk, SUM(ss_quantity) AS storeTotal 
FROM store_sales
WHERE ss_sold_date_sk >= START_DATE AND ss_sold_date_sk <= END_DATE
GROUP BY ss_store_sk ORDER BY storeTotal DESC LIMIT N) AS myStore

JOIN

(SELECT ss_store_sk, ss_item_sk, SUM(ss_quantity) AS itemTotal 
FROM store_sales
WHERE ss_sold_date_sk >= START_DATE AND ss_sold_date_sk <= END_DATE
GROUP BY ss_store_sk, ss_item_sk) AS mySales

ON myStore.ss_store_sk == mySales.ss_store_sk
) AS ranks

WHERE rank <= M
ORDER BY ranks.s4 DESC, ranks.rank ASC;
```

### Question 1C - Which days among range have highest total value of ss_net_paid_inc_tax?

Copy and run this `hive` query:\
(K = 10, Start Date = 2451146, End Date = 2452268)

```sql
SELECT concat('ss_sold_date_sk_',ss_sold_date_sk), SUM(ss_net_paid_inc_tax) as taxSum
FROM store_sales
WHERE ss_sold_date_sk >= 2451392 AND ss_sold_date_sk <= 2451894
GROUP BY (ss_sold_date_sk)
ORDER BY taxSum DESC
LIMIT 10;
```

Expected output:
```bash
ss_sold_date_sk_2451546 269011207.69
ss_sold_date_sk_2451522 218299535.32
ss_sold_date_sk_2451544 216896013.28
ss_sold_date_sk_2451537 215618328.78
ss_sold_date_sk_2451521 215392369.46
ss_sold_date_sk_2451533 214772795.73
ss_sold_date_sk_2451532 214618693.96
ss_sold_date_sk_2451851 213953384.07
ss_sold_date_sk_2451891 213918317.29
ss_sold_date_sk_2451880 213849908.61
```

General query: \
*(Enter values for K, START_DATE and END_DATE within the query below.)*
```sql
SELECT concat('ss_sold_date_sk_',ss_sold_date_sk), SUM(ss_net_paid_inc_tax) as taxSum
FROM store_sales
WHERE ss_sold_date_sk >= START_DATE AND ss_sold_date_sk <= END_DATE
GROUP BY (ss_sold_date_sk)
ORDER BY taxSum DESC
LIMIT K;
```

### Question 2 - Joins

Copy and run this `hive` query:\
(K = 10, Start Date = 2451146, End Date = 2452268)

```sql
SELECT ss_store_sk, s_floor_space, sum(ss_net_paid) as net 
FROM store_sales
JOIN store ON (ss_store_sk == s_store_sk)
WHERE ss_sold_date_sk >= 2451146 AND ss_sold_date_sk <= 2452268 AND ss_store_sk IS NOT NULL
GROUP BY ss_store_sk, s_floor_space
ORDER BY net DESC, s_floor_space DESC
LIMIT 10; 
```

Expected output:
```bash
92      8573853 2039983202.07
50      7825489 2036624698.89
8       6995995 2032733764.65
32      6131757 2028710419.82
4       9341467 2028145948.34
94      9599785 2026137085.11
38      5813235 2024183575.31
82      6035829 2023916557.05
97      7232715 2023659816.02
112     6081074 2023012491.83
```

General query: \
*(Enter values for K, START_DATE and END_DATE within the query below.)*
```sql
SELECT ss_store_sk, s_floor_space, sum(ss_net_paid) as net 
FROM store_sales
JOIN store ON (ss_store_sk == s_store_sk)
WHERE ss_sold_date_sk >= START_DATE AND ss_sold_date_sk <= END_DATE AND ss_store_sk IS NOT NULL
GROUP BY ss_store_sk, s_floor_space
ORDER BY net DESC, s_floor_space DESC
LIMIT K; 
```