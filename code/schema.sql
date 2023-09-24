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