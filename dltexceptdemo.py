CREATE STREAMING TABLE orders_bronze
comment "ingesting from landing to bronze layer"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * , 
_metadata.file_name as filename,
current_timestamp() as load_time
FROM cloud_files('/mnt/landing/orders', 'csv', map("cloudFiles.inferColumnTypes","true"));

CREATE STREAMING TABLE customers_bronze
AS
SELECT * ,
_metadata.file_name as filename,
current_timestamp() as load_time
FROM cloud_files('/mnt/landing/customers', 'csv', map("cloudFiles.inferColumnTypes","true"));



CREATE STREAMING TABLE orders_silver_cleaned(
	constraint valid_order EXPECT (order_id is NOT NULL) ON VIOLATION DROP ROW,
	constraint valid_customer EXPECT (customer_id is NOT NULL) ON VIOLATION DROP ROW,
	constraint vaild_amount EXPECT (total_amount > 0) ON VIOLATION FAIL UPDATE
) AS SELECT orderid AS order_id, orderdate as order_date, customerid as customer_id, totalamount as total_amount, status, 
filename as file_name, 
load_time
FROM STREAM(live.orders_bronze);


CREATE STREAMING TABLE customers_silver_cleaned(
	constraint valid_customer EXPECT (customer_id is NOT NULL) ON VIOLATION DROP ROW,
	constraint customer_since EXPECT (customer_since is NOT NULL)
) 
AS SELECT CUSTOMERID AS customer_id, customername as customer_name, address as city, dateofbirth as dob, registrationdate as customer_since, 
filename as file_name, 
load_time
FROM STREAM(live.customers_bronze);


create streaming table customers_silver;

apply changes into live.customers_silver
from stream(live.customers_silver_cleaned)
keys(customer_id)
sequence by load_time
COLUMNS * EXCEPT
(customer_name)
stored as scd type 2;

create streaming table orders_silver;

apply changes into live.orders_silver
from stream(live.orders_silver_cleaned)
keys(order_id)
sequence by load_time;

create materialized view orders_customers_mapped(
  CONSTRAINT no_missing_records EXPECT (c.customer_id IS NOT NULL)
)
as select o.order_id, o.order_date, o.total_amount ,c.customer_id, c.city
from live.orders_silver o
left outer join live.customers_silver c
on o.customer_id = c.customer_id;

create materialized view city_wise_sales_gold
as select city, sum(total_amount) as total_sales, count(1) as total_orders
from live.orders_silver o
left outer join live.customers_silver c
on o.customer_id = c.customer_id
group by city;