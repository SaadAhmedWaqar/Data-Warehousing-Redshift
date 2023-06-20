import sys
import boto3 
import psycopg2
import json

from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

glue_client = boto3.client("glue")

fact_table_sql = """

insert into fct_order_details (orderid, product_skey, customer_skey, store_skey, date_key, quantity,       price,       total    ,grand_total )
select distinct sfod.orderid, dp.product_skey, dc.customer_skey, ds.store_skey, so.o_date, sfod.quantity, sfod.price, sfod.total   ,sfod.grand_total          
from stg_fct_order_details sfod
join stg_orders so on sfod.orderid = so.orderid
join dim_products dp on sfod.productid = dp.productid -- joining dim products & stg fct table on productID 
join dim_customers dc on sfod.customerid = dc.customerid -- joining dim customer & stg fct table on customerID
join dim_stores ds  on sfod.storeid = ds.storeid ;

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
        
def fact_population (table_name,redshift_conn):
    
    if table_name == "OrderDetails":
        with redshift_conn.cursor() as cursor:
            try:
                cursor.execute(fact_table_sql)
                redshift_conn.commit()
                print ('FACT Population script executed')
            
            except psycopg2.Error as e:
                print(f"Error for {table_name}: while population  {str(e)}")
                redshift_conn.rollback()
                raise Exception("Population FAILED")
            finally:
                cursor.close()
                redshift_conn.close()
                
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

params = workflow_params()
bucket = params['bucket']
key = params['key']
table_name = params['table_name']

params = workflow_params()

secrets = get_secret ()
fact_population (table_name,redshift_conn)


        