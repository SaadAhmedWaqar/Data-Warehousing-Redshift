
---------------------------------------------BEGIN---------------------------------------------------
begin;


drop table if exists stg_stores ;
drop table if exists stg_customers ;
drop table if exists stg_orders ; 
drop table if exists stg_orderdetails ; 
drop table if exists stg_products ; 
drop table if exists stg_fct_order_details ; 
----------------------------------------------STORES------------------------------------------------

CREATE TABLE stg_stores (
	storeid int4 NOT NULL,
	storename varchar(50) NOT NULL,
	address varchar(64) NOT NULL,
	city varchar(50) NOT NULL,
	state varchar(50) NOT NULL,
	zipcode int4 NOT NULL,
    PRIMARY KEY (storeid),
    unique (storeid)
);     

insert into stg_stores (storeid, storename, address, city, state, zipcode)
select DISTINCT storeid, storename, address, city, state, zipcode
from stores; 

----------------------------------------------CUSTOMERS------------------------------------------------

CREATE TABLE stg_customers (
	customerid int4 NOT NULL,
	firstname varchar(50) NOT NULL,
	lastname varchar(50),
	email varchar(50) NOT NULL,
	address varchar(64) NOT NULL,
	city varchar(50) NOT NULL,
	state varchar(50) NOT NULL,
	zipcode int4 NOT NULL,
--	lastmodified varchar(50) ,
    PRIMARY KEY (customerid),
    UNIQUE (customerid)
);


insert into stg_customers (customerid, firstname, lastname, email, address, city, state, zipcode)
select distinct customerid, firstname, lastname, email, address, city, state, zipcode
from customers;


----------------------------------------------ORDERS------------------------------------------------
CREATE TABLE stg_orders (
	orderid int4  NOT NULL,
	orderdate date NOT NULL,
	o_date varchar(10)
);

insert into stg_orders (orderid, orderdate,  o_date)
select  orderid,orderdate, NULL
from orders;

update stg_orders 
set o_date = to_char(orderdate, 'yyyymmdd')
where orderdate is not null;

----------------------------------------------PRODUCTS------------------------------------------------

CREATE TABLE stg_products (
	productid int4 NOT NULL,
	productname varchar(50) NOT NULL,
	category varchar(50) NOT NULL,
	description varchar(128) NOT NULL,
	price float4 NULL,
    primary key (productid),
    unique (productid)
);


insert into stg_products (productid, productname, category, description, price)
select productid,productname,category, description, price
from products;


----------------------------------------------STAGING FACT TABLE------------------------------------------------

CREATE TABLE stg_fct_order_details (
  orderid int primary key not null,
  productid int not null,
  customerid int not null, 
  storeid int not null,
  quantity int not null,
  price decimal (7,2) not null,
  total decimal (7,2) not null,
  grand_total  decimal (7,2) not null

);


insert into stg_fct_order_details (orderid, productid, customerid, storeid, quantity, price, total, grand_total )
select  o.orderid, od.productid, o.customerid, o.storeid, od.quantity, od.price, od.quantity*od.price, o.totalprice
from orders o
join orderdetails od on o.orderid = od.orderid;


end;
---------------------------------------------END---------------------------------------------------
