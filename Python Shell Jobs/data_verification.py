import sys
import boto3 
import psycopg2
import json

from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

glue_client = boto3.client("glue")

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
    except ClientError as e:
        raise Exception("Secret Manager get_secret_value failed")
    secret = get_secret_value_response['SecretString']
    print (secret)
    return secret


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
        
def schema_verification (table_name,redshift_conn):
 
    t_names = ['Customers', 'Stores', 'Products', 'Orders']
    
    table_columns = {
        'Customers': ['customerid', 'firstname', 'lastname', 'email', 'address', 'city', 'state', 'zipcode','lastmodified'],
        'Stores': ['storeid', 'storename', 'address', 'city', 'state', 'zipcode','lastmodified'],
        'Products': ['productid','productname','category','description','price','lastmodified'],
        'Orders': ['orderid','customerid','storeid','orderdate','totalprice','lastmodified']
        
    }
    not_null_columns = table_columns [table_name]
    unique_key_columns = [table_columns[table_name][0]]
     
    if table_name not in t_names:
        print("Invalid table name")
        raise Exception("Unknown table uploaded")
        return False
        
    with redshift_conn.cursor() as cursor:
        try:
            print ('into VALIDATE cursor')
            # checking for empty table
            temp = table_name.lower
            
            sql = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(sql)
            result = cursor.fetchone()
            count = result[0]
            if count > 0:
                print(f"Empty check PASSED for table {table_name}. Rows: {count}")
            else:
                print(f"Empty check FAILED for table {table_name}. No rows found.")
                raise Exception("Empty table uploaded")
                return False
               # checking for null entries   
            for i, column in enumerate(not_null_columns):
                
                null_check = f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE {column} IS NULL;
                """
                cursor.execute(null_check)
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"NULL check FAILED {count} rows with NULL value in column {column} of table {table_name}")
                    raise Exception(f"Not Null constraints violation in Table: {table_name}")
                    return False
                    print(f" {i} Column {column} checked ")
            
            unique_key = (unique_key_columns[0])
            
            unique_check = f"""
            SELECT {unique_key}, COUNT(*)
            FROM {table_name}
            GROUP BY {unique_key}
            HAVING COUNT(*) > 1;
            """
            cursor.execute (unique_check)
            duplicate_rows = cursor.fetchall()
            print(f"Duplicate rows are {duplicate_rows}")
            
            if duplicate_rows:
                print(f"Duplicate check FAILED. Duplicate rows found for the unique key {unique_key} in table {table_name}")
                raise Exception(f"Unique Key constraints (duplication) violation in Table: {table_name}")
                return False
        except psycopg2.Error as e:
            print(f"Error loading data into table {table_name}: {str(e)}")
            raise Exception("Data loading failed") 
    return True
                
                
def copy_into_redshift(bucket, key, table_name):
    print("in copy_into_redshift")
    
    credentials = json.loads(get_secret())
    redshift_host = credentials["host"]
    redshift_port = credentials["port"]
    redshift_user = credentials["user"]
    redshift_password = credentials["password"]
    redshift_database  = credentials["database"]
 
    redshift_conn = psycopg2.connect(host = redshift_host, port = int(redshift_port) ,user = redshift_user , password = redshift_password, database = redshift_database)

    redshift_copy_command = f"""
    truncate table {table_name}; 
    COPY {table_name} 
    FROM 's3://{bucket}/{key}' 
    {IAM}
    FORMAT AS CSV 
    DELIMITER ',' 
    IGNOREHEADER 1;
    """
    with redshift_conn.cursor() as cursor:
        try:
            print ('into COPY cursor')
            
            cursor.execute(redshift_copy_command)
            redshift_conn.commit()
            print ('COPY script commited')
            if (table_name != "OrderDetails"):
                if schema_verification (table_name, redshift_conn) and table_name !="OrderDetails":
                    return True
                else:
                    print("Validation FAILED")
                    raise Exception("Data Validation FAILED")
                
        except psycopg2.Error as e:
            print(f"Error for {table_name} table: {str(e)}")
            raise Exception(f"Data loading failed while copying data into redshift due to {str(e)}")

        finally:
            redshift_conn.rollback()
            redshift_conn.close()

      
        

# starts here

args = getResolvedOptions(sys.argv, ['SecretName', 'SecretRegionName', 'SecretService' , 'IAM']) # these args come from env vars
s_name = args['SecretName']
r_name = args['SecretRegionName']
service = args['SecretService']
IAM = args['IAM']

params = workflow_params()
bucket = params['bucket']
key = params['key']
table_name = params['table_name']

secrets = get_secret ()

copy_into_redshift (bucket, key, table_name)


        