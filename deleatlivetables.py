CREATE STREAMING TABLE orders_bronze
AS 
SELECT  
*
,_metadata.file_name as file_name
,current_timestamp as updated_on
FROM cloudFiles('<path>','<format>',map('cloudFiles.inferSchema',True))



CREATE STREAMING TABLE customers_bronze
AS 
SELECT  
*
,_metadata.file_name as file_name
,current_timestamp as updated_on
FROM cloud_files('<path>','<format>',map('cloudFiles.inferSchema',True))


CREATE STREAMING TABLE orders_cleaned
(
    CONSTRAINT <name>
)
AS 
SELECT  
*
,_metadata.file_name as file_name
,current_timestamp as updated_on
FROM cloud_files('<path>','<format>',map('cloudFiles.inferSchema',True))


CREATE STREAMING TABLE orders_silver_cleaned (
	CONSTRAINT valid_order EXPECT (order_id is NOT NULL) ON VIOLATION DROP ROW,
	CONSTRAINT valid_customer EXPECT (customer_id is NOT NULL) ON VIOLATION DROP ROW	
) as 
SELECT orderid as order_id,
orderdate as order_date,
customerid as customer_id,
totalamount as total_amount,
status,
filename as file_name,
load_time
FROM STREAM(live.orders_bronze);



CREATE STREAMING TABLE customers_silver_cleaned(
	CONSTRAINT valid_customer EXPECT (customer_id is NOT NULL) ON VIOLATION DROP ROW
) as
SELECT customerid as customer_id,
customername as customer_name,
address as city,
dateofbirth as dob,
registrationdate as customer_since,
filename as file_name,
load_time
from STREAM(live.customers_bronze);


# merge changes into orders_silver table

CREATE STREAMING TABLE orders_silver;

APPLY CHANGES INTO orders_silver
FROM STREAM(live.orders_silver_cleaned)
APPLY AS DELETE WHEN
orders_status = "DELETE"
keys(order_id)
sequence by updated_on;

# merge changes into orders_silver table

CREATE STREAMING TABLE orders_silver;

APPLY CHANGES INTO customers_silver
FROM STREAM(live.customers_silver_cleaned)
keys(cusotmer_id)
APPLY AS DELETE WHEN
operation = "DELETE"
sequence by updated_on
stored as scd type2;


#  crreate a materliazed view

CREATE MATERIALIZED VIEW city_wise_sales_gold
as
SELECT city, sum(total_amount) as total_sales
from live.orders_silver o
join live.customers_silver c
on o.customer_id = c.customer_id
group by city;
