import sys
import boto3 
import psycopg2
import json

from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

glue_client = boto3.client("glue")

customers_sql = """
drop table if exists stg_customers ;

CREATE TABLE stg_customers (
	customerid int4 NOT NULL,
	firstname varchar(50) NOT NULL,
	lastname varchar(50),
	email varchar(50) NOT NULL,
	address varchar(64) NOT NULL,
	city varchar(50) NOT NULL,
	state varchar(50) NOT NULL,
	zipcode int4 NOT NULL,
    PRIMARY KEY (customerid),
    UNIQUE (customerid)
);

insert into stg_customers (customerid, firstname, lastname, email, address, city, state, zipcode)
select customerid, firstname, lastname, email, address, city, state, zipcode
from customers;

UPDATE dim_customers
SET end_date = DATE(GETDATE()), active = false 
WHERE customerid IN (SELECT customerid FROM stg_customers) AND end_date = '9999-01-01';

INSERT INTO dim_customers (customerid, firstname, lastname, email, address, city, state, zipcode,active,start_date,    end_date)
SELECT         customerid, firstname, lastname, email, address, city, state, zipcode, true ,DATE(GETDATE()),  date ('9999-01-01')
FROM stg_customers ;
"""

stores_sql = """
drop table if exists stg_stores ;

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
select  storeid, storename, address, city, state, zipcode
from stores; 

UPDATE dim_stores
SET end_date = DATE(GETDATE()), active = false 
WHERE storeid IN (SELECT storeid FROM stg_stores) AND end_date = '9999-01-01';

INSERT INTO dim_stores (storeid, storename, address, city, state, zipcode, active, start_date,      end_date)
SELECT             storeid, storename, address, city, state, zipcode, true , DATE(GETDATE()),  date ('9999-01-01')
FROM stg_stores ;
"""

products_sql = """
drop table if exists stg_products ;

CREATE TABLE stg_products (
	productid int4 NOT NULL,
	productname varchar(50) NOT NULL,
	category varchar(50) NOT NULL,
	description varchar(128) NOT NULL,
	price float4 NULL,
--	lastmodified varchar(50)  NULL,
    primary key (productid),
    unique (productid)
);

insert into stg_products (productid, productname, category, description, price)
select productid,productname,category, description, price
from products;

UPDATE dim_products
SET end_date = DATE(GETDATE()), active = false 
WHERE productid IN (SELECT productid FROM stg_products) AND end_date = '9999-01-01';

INSERT INTO dim_products (productid, productname, category, description, price, active, start_date,        end_date)
SELECT            productid, productname, category, description, price,  true , DATE(GETDATE()),  date ('9999-01-01')
FROM stg_products ;
"""


stg_orders_sql = """
drop table if exists stg_orders ;

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
"""

stg_fct_order_details_sql = """
drop table if exists stg_fct_order_details ;

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
"""

def get_secret():
    secret_name = s_name
    region_name = r_name
    session = boto3.session.Session()
    client = session.client(
        service_name=service,
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = get_secret_value_response['SecretString']
        return secret
    except ClientError as e:
        raise Exception("Secret Manager get_secret_value failed")
    
def staging_quries (table_name,redshift_conn):
    print (table_name)
    if table_name !="Orders" and table_name !="OrderDetails":    # if we get a dimention table 
        with redshift_conn.cursor() as cursor:
            try:
                if table_name == 'Customers':
                    cursor.execute(customers_sql)
                    redshift_conn.commit()
                    print ('Customers script executed') 
                elif table_name == 'Products':
                    cursor.execute(products_sql)
                    redshift_conn.commit()
                    print ('Products script executed') 
                elif table_name == 'Stores':
                    cursor.execute(stores_sql)
                    redshift_conn.commit()
                    print ('Stores script executed')
            
            except psycopg2.Error as e:
                print(f"Error for {table_name} table during staging & upsert: {str(e)}")
                raise Exception("Upsert FAILED")
            finally:
                cursor.close()
                redshift_conn.close()
            
    elif  table_name =="OrderDetails":                 
        with redshift_conn.cursor() as cursor:
            try:
                cursor.execute(stg_fct_order_details_sql)
                redshift_conn.commit()
                print ('Staging Fact OrderDetails script executed')
            
            except psycopg2.Error as e:
                print(f"Error for {table_name}: while creating stg_fct_orderdetails  {str(e)}")
                raise Exception("Creation of Staging Fact table FAILED")
            finally:
                cursor.close()
                redshift_conn.close()
            
    elif  table_name == "Orders":
        with redshift_conn.cursor() as cursor:
            try:
                cursor.execute(stg_orders_sql)
                redshift_conn.commit()
                print ('STG Orders script executed')
    
            except psycopg2.Error as e:
                print(f"Error for {table_name}: while creating stg_orders  {str(e)}")
                raise Exception("creating stg orders FAILED")
            finally:
                cursor.close()
                redshift_conn.close()
            
def workflow_params ():
    print ('Fetching Params')
    try:
        args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
        workflow_name = args['WORKFLOW_NAME']
        workflow_run_id = args['WORKFLOW_RUN_ID']
        workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]
        print(f"Workflow properties: Name = {workflow_name},\nParams = {workflow_params}")
        return workflow_params
    except exception as e:
        print ('Something went wrong while fetching workflow params:',e)
        raise e

# STARTS HERE 
args = getResolvedOptions(sys.argv, ['SecretName', 'SecretRegionName', 'SecretService'])
s_name = args['SecretName']
r_name = args['SecretRegionName']
service = args['SecretService']

credentials = json.loads(get_secret())
redshift_host = credentials["host"]
redshift_port = credentials["port"]
redshift_user = credentials["user"]
redshift_password = credentials["password"]
redshift_database  = credentials["database"]

redshift_conn = psycopg2.connect(
    host = redshift_host,
    port = int(redshift_port),
    user = redshift_user,
    password = redshift_password,
    database = redshift_database
    )

secrets = get_secret ()
params = workflow_params()
bucket = params['bucket']
key = params['key']
table_name = params['table_name']
staging_quries (table_name,redshift_conn)




