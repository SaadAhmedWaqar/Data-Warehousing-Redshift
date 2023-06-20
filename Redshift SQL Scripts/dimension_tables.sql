----------------------------------------------CUSTOMERS------------------------------------------------

create table dim_customers(
customer_skey int identity(1,1) not null,
customerid int not null,
firstname varchar(50) not null,
lastname varchar(50),
email varchar(50) not null,
address varchar(64) not null,
city varchar(50) not null,
state varchar(50) not null,
zipcode int not null,
active bool not null,
start_date date not null,
end_date date,
primary key (customer_skey)
);

-- updating previous records which have been modified

UPDATE dim_customers
SET end_date = DATE(GETDATE()), active = false 
WHERE customerid IN (SELECT customerid FROM stg_customers) AND end_date = '9999-01-01';


-- inserting only new records
INSERT INTO dim_customers (customerid, firstname, lastname, email, address, city, state, zipcode,active,start_date,    end_date)
SELECT     distinct        customerid, firstname, lastname, email, address, city, state, zipcode, true ,DATE(GETDATE()),  date ('9999-01-01')
FROM stg_customers ;

----------------------------------------------PRODUCTS------------------------------------------------

create table dim_products(
product_skey int identity(1,1) not null,
productid int not null,
productname varchar (50) not null,
category varchar (50) not null,
description varchar (128) not null,
price decimal (7,2),
active bool not null,
start_date date not null,
end_date date,
primary key (product_skey)
);


-- updating previous records which have been modified

UPDATE dim_products
SET end_date = DATE(GETDATE()), active = false 
WHERE productid IN (SELECT productid FROM stg_products) AND end_date = '9999-01-01';


-- inserting only new records
INSERT INTO dim_products (productid, productname, category, description, price, active, start_date,        end_date)
SELECT     distinct       productid, productname, category, description, price,  true , DATE(GETDATE()),  date ('9999-01-01')
FROM stg_products ;

----------------------------------------------STORES------------------------------------------------

create table dim_stores(
store_skey int identity(1,1) not null,
storeid int not null,
storename varchar(50) not null,
address varchar(64) not null,
city varchar(50) not null,
state varchar(50) not null,
zipcode int not null,
active bool not null,
start_date date not null,
end_date date ,
primary key (store_skey)
);


UPDATE dim_stores
SET end_date = DATE(GETDATE()), active = false 
WHERE storeid IN (SELECT storeid FROM stg_stores) AND end_date = '9999-01-01';


INSERT INTO dim_stores (storeid, storename, address, city, state, zipcode, active, start_date,      end_date)
SELECT     distinct        storeid, storename, address, city, state, zipcode, true , DATE(GETDATE()),  date ('9999-01-01')
FROM stg_stores ;


----------------------------------------------FCT ORD DETAILS----------------------------------------------

CREATE TABLE fct_order_details (
  orderid int primary key not null,
  product_skey int not null,
  foreign key (product_skey) references dim_products (product_skey),
  customer_skey int not null, 
  foreign key (customer_skey) references dim_customers (customer_skey),
  store_skey int not null,
  foreign key (store_skey) references dim_stores (store_skey),
  date_key varchar(10) not null,
  foreign key (date_key) references dim_date (date_key),
  quantity int not null,
  price decimal (7,2) not null,
  total decimal (7,2) not null,
  grand_total  decimal (7,2) not null

);


insert into fct_order_details (orderid, product_skey, customer_skey, store_skey, date_key, quantity,       price,       total,     grand_total )
select distinct sfod.orderid, dp.product_skey, dc.customer_skey, ds.store_skey, so.o_date, sfod.quantity, sfod.price, sfod.total   ,sfod.grand_total          
from stg_fct_order_details sfod
join stg_orders so on sfod.orderid = so.orderid
join dim_products dp on sfod.productid = dp.productid -- joining dim products & stg fct table on productID 
join dim_customers dc on sfod.customerid = dc.customerid -- joining dim customer & stg fct table on customerID
join dim_stores ds  on sfod.storeid = ds.storeid ;-- joining dim store & stg fct table on storeID
