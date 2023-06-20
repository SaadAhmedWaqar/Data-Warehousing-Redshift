---------------------------------------------BEGIN---------------------------------------------------

begin;

drop table if exists stores ;
drop table if exists customers ;
drop table if exists orders ; 
drop table if exists orderdetails ; 
drop table if exists products ;


CREATE TABLE stores (
	storeid int4 NOT NULL,
	storename varchar(50) NOT NULL,
	address varchar(64) NOT NULL,
	city varchar(50) NOT NULL,
	state varchar(50) NOT NULL,
	zipcode int4 NOT NULL,
	lastmodified varchar(50) NOT NULL,
    PRIMARY KEY (storeid),
    unique (storeid)
);

COPY stores
FROM 's3://saad-data-assignment/Sales_Data/Stores/Stores.csv'
IAM_ROLE 'arn:aws:iam::414432075221:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T143853'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1;

CREATE TABLE customers (
	customerid int4 NOT NULL,
	firstname varchar(50) NOT NULL,
	lastname varchar(50),
	email varchar(50) NOT NULL,
	address varchar(64) NOT NULL,
	city varchar(50) NOT NULL,
	state varchar(50) NOT NULL,
	zipcode int4 NOT NULL,
	lastmodified varchar(50) NOT NULL,
    PRIMARY KEY (customerid),
    UNIQUE (customerid)
);

COPY customers
FROM 's3://saad-data-assignment/Sales_Data/Customers/Customers.csv'
IAM_ROLE 'arn:aws:iam::414432075221:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T143853'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1;

CREATE TABLE orders (
	orderid int4  NOT NULL,
	customerid int4 NOT NULL,
	storeid int4 NOT NULL,
	orderdate date NOT NULL,
	totalprice float4 NOT NULL,
	lastmodified varchar(50) NOT NULL,
    primary key (orderid),
    unique (orderid)
);

COPY orders
FROM 's3://saad-data-assignment/Sales_Data/Orders/Orders.csv'
IAM_ROLE 'arn:aws:iam::414432075221:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T143853'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1;

CREATE TABLE orderdetails (
	orderid int4 NOT NULL,
	productid int4 NOT NULL,
	quantity int4 NOT NULL,
	price float4 NOT NULL,
	lastmodified varchar(50) NOT NULL,
    unique (orderid,productid)
);

COPY orderdetails
FROM 's3://saad-data-assignment/Sales_Data/OrderDetails/OrderDetails.csv'
IAM_ROLE 'arn:aws:iam::414432075221:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T143853'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1;

CREATE TABLE products (
	productid int4 NOT NULL,
	productname varchar(50) NOT NULL,
	category varchar(50) NOT NULL,
	description varchar(128) NOT NULL,
	price float4 NULL,
	lastmodified varchar(50) NOT NULL,
    primary key (productid),
    unique (productid)
);

COPY products
FROM 's3://saad-data-assignment/Sales_Data/Products/Products.csv'
IAM_ROLE 'arn:aws:iam::414432075221:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T143853'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1;

end;
---------------------------------------------END---------------------------------------------------






--------------------------------------------------DATE DIM-------------------------------------------
CREATE TABLE dim_date (
	date_key varchar(10) NOT NULL,
	full_date varchar(50) NOT NULL,
	day_name varchar(50) NOT NULL,
	day_of_month int4 NOT NULL,
	week_of_month int4 NOT NULL,
	month_name varchar(50) NOT NULL,
	month_num int4 NOT NULL,
	quarter varchar(50) NOT NULL,
	"year" int4 NOT NULL,
	PRIMARY KEY (date_key)
);


COPY dim_date
FROM 's3://saad-data-assignment/data_new/dim_date.csv'
IAM_ROLE 'arn:aws:iam::414432075221:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T143853'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1;
