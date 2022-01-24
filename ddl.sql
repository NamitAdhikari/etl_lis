create or replace table STG_COUNTRY(
id NUMBER,
country_desc VARCHAR(256)
);

create or replace table STG_REGION
(
id NUMBER,
country_id NUMBER,
region_desc VARCHAR(256)
)
;

create or replace table STG_STORE
(
id NUMBER,
region_id NUMBER,
store_desc VARCHAR(256)
)
;

create or replace table STG_CATEGORY
(
id NUMBER,
category_desc VARCHAR(1024)
);

create or replace table STG_SUBCATEGORY
(
id NUMBER,
category_id NUMBER,
subcategory_desc VARCHAR(256)
 
);

create or replace table STG_PRODUCT
(
id NUMBER,
subcategory_id NUMBER,
product_desc VARCHAR(256)
);

create or replace table STG_CUSTOMER
(
id NUMBER,
customer_first_name VARCHAR(256),
customer_middle_name VARCHAR(256),
customer_last_name VARCHAR(256),
customer_address VARCHAR(256) 

);

create or replace table STG_SALES
(
id NUMBER,
store_id NUMBER NOT NULL,
product_id NUMBER NOT NULL,
customer_id NUMBER,
transaction_time TIMESTAMP,
quantity NUMBER,
amount NUMBER(20,2),
discount NUMBER(20,2)
);











create or replace table d_country_tmp(
  country_key number autoincrement start 1 increment 1,
  country_id number(38,0),
  country_desc varchar(200),
  insert_time datetime,
  update_time datetime);
  
create or replace table d_region_tmp(
  region_key number autoincrement start 1 increment 1,
  region_id number(38,0) ,
  country_id number,
  region_desc varchar(200),
  insert_time datetime,
  update_time datetime
);

create or replace table d_store_tmp(
  store_key number autoincrement start 1 increment 1,
  store_id number(38,0),
  region_id number,
  store_desc varchar(200),
  current_flag char(1),
  effective_from datetime,
  effective_to datetime,
  insert_time datetime, 
  update_time datetime
);

create or replace table d_category_tmp(
  category_key number autoincrement start 1 increment 1,
  category_id number(38,0),
  category_desc varchar(1024),
  insert_time datetime, 
  update_time datetime,
  active_flag char(1),
  start_date datetime,
  end_date datetime
);

create or replace table d_subcategory_tmp(
  subcategory_key number autoincrement start 1 increment 1,
  subcategory_id number(38,0),
  subcategory_desc varchar(256),
  category_id number,
  insert_time datetime, 
  update_time datetime,
  active_flag char(1),
  start_date datetime,
  end_date datetime
);

create or replace table d_product_tmp(
  product_key number autoincrement start 1 increment 1,
  product_id number(38,0),
  subcategory_id number(38,0),
  product_desc varchar(256),
  insert_time datetime, 
  update_time datetime,
  active_flag char(1),
  start_date datetime,
  end_date datetime
);

create or replace table d_customer_tmp(
  customer_key number autoincrement start 1 increment 1,
  customer_id number(38,0),
  customer_first_name varchar(256),
  customer_last_name varchar(256),
  customer_middle_name varchar(256),
  customer_address varchar(256),
  insert_time datetime, 
  update_time datetime,
  active_flag char(1),
  start_date datetime,
  end_date datetime
);

create or replace table f_sales_tmp(
  sales_key number autoincrement start 1 increment 1,
  sales_id number(38,0),
  store_id number,
  product_id number,
  customer_id number,
  transaction_time datetime,
  quantity number(38,0),
  amount number(20,2),
  discount number(20,2),
  insert_time datetime, 
  update_time datetime,
  start_date datetime,
  end_date datetime
);